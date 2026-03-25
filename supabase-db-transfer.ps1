# supabase_transfer_and_prefix.ps1
# Transfers the public schema from one Postgres/Supabase database to another.
# After restore, imported tables are renamed with a prefix.
#
# Behavior:
# 1) Fresh target:
#    - dump source public schema
#    - patch dump
#    - dump source public data
#    - restore into target
#    - rename imported tables with prefix
#
# 2) Resume mode:
#    - if ALL source tables already exist unprefixed in target,
#      skip dump/restore and only rename them
#
# 3) Partial target state:
#    - if SOME source tables already exist unprefixed in target but not all,
#      stop with an error
#
# Requirements:
# - pg_dump in PATH
# - psql in PATH
# - PowerShell 5.1+ or PowerShell 7+

$ErrorActionPreference = "Stop"

# =========================================================
# CONFIGURE HERE
# =========================================================
# Passwords with special characters must be URL-encoded.
$OldDbUrl = 'postgresql://SOURCE_USER:SOURCE_PASSWORD@SOURCE_HOST:5432/postgres'
$NewDbUrl = 'postgresql://TARGET_USER:TARGET_PASSWORD@TARGET_HOST:5432/postgres'

# Lowercase is recommended to avoid quoted identifiers in PostgreSQL.
$TablePrefix = 'app_'

# If true and ALL source tables already exist unprefixed in target,
# the script skips transfer and only renames those existing tables.
$ResumeFromExistingTargetTables = $true

$WorkDir = Join-Path $PSScriptRoot 'supabase-db-transfer'

# =========================================================
# HELPER FUNCTIONS
# =========================================================
function Test-CommandExists {
    param([string]$CommandName)

    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Command '$CommandName' was not found. Please install PostgreSQL client tools and add them to PATH."
    }
}

function Run-ExternalCommand {
    param(
        [string]$FilePath,
        [string[]]$Arguments
    )

    Write-Host ""
    Write-Host ">> $FilePath $($Arguments -join ' ')" -ForegroundColor Cyan

    & $FilePath @Arguments

    if ($LASTEXITCODE -ne 0) {
        throw "Error while running '$FilePath'. ExitCode: $LASTEXITCODE"
    }
}

function Run-ExternalCommandCapture {
    param(
        [string]$FilePath,
        [string[]]$Arguments
    )

    Write-Host ""
    Write-Host ">> $FilePath $($Arguments -join ' ')" -ForegroundColor DarkCyan

    $output = & $FilePath @Arguments 2>&1
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        $text = ($output | Out-String).Trim()
        throw "Error while running '$FilePath'. ExitCode: $exitCode`n$text"
    }

    return $output
}

function Quote-SqlLiteral {
    param([string]$Value)
    return "'" + ($Value -replace "'", "''") + "'"
}

function Quote-SqlIdentifier {
    param([string]$Value)
    return '"' + ($Value -replace '"', '""') + '"'
}

function Get-ExtensionSchema {
    param(
        [string]$DbUrl,
        [string]$ExtensionName
    )

    $query = @"
SELECT n.nspname
FROM pg_extension e
JOIN pg_namespace n ON n.oid = e.extnamespace
WHERE e.extname = '$ExtensionName';
"@

    $result = Run-ExternalCommandCapture -FilePath 'psql' -Arguments @(
        '--dbname', $DbUrl,
        '-At',
        '-c', $query
    )

    $schema = ($result | Out-String).Trim()
    if ([string]::IsNullOrWhiteSpace($schema)) {
        return $null
    }

    return $schema
}

function Get-PublicTables {
    param([string]$DbUrl)

    $query = @"
SELECT c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind IN ('r','p')
ORDER BY c.relname;
"@

    $raw = Run-ExternalCommandCapture -FilePath 'psql' -Arguments @(
        '--dbname', $DbUrl,
        '-At',
        '-c', $query
    )

    return @(
        $raw |
        ForEach-Object { "$_".Trim() } |
        Where-Object { $_ -ne '' }
    )
}

function Write-RenameSqlFile {
    param(
        [string[]]$TableNames,
        [string]$Prefix,
        [string]$OutputPath
    )

    $escapedPrefix = $Prefix -replace "'", "''"
    $tableArraySql = "ARRAY[" + (($TableNames | ForEach-Object { Quote-SqlLiteral $_ }) -join ", ") + "]::text[]"

    $renameSqlTemplate = @'
DO $$
DECLARE
    v_prefix   text   := '__PREFIX__';
    v_tables   text[] := __TABLES__;
    v_old_name text;
    v_new_name text;
    s record;
BEGIN
    FOREACH v_old_name IN ARRAY v_tables LOOP
        IF v_old_name LIKE v_prefix || '%' THEN
            CONTINUE;
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind IN ('r','p')
              AND c.relname = v_old_name
        ) THEN
            RAISE EXCEPTION 'Table not found: public.%', v_old_name;
        END IF;

        v_new_name := v_prefix || v_old_name;

        IF EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind IN ('r','p')
              AND c.relname = v_new_name
        ) THEN
            RAISE EXCEPTION 'Target table name already exists: public.%', v_new_name;
        END IF;

        EXECUTE format('ALTER TABLE public.%I RENAME TO %I', v_old_name, v_new_name);

        FOR s IN
            SELECT seq.relname AS seq_name
            FROM pg_class seq
            JOIN pg_namespace seq_ns ON seq_ns.oid = seq.relnamespace
            JOIN pg_depend dep       ON dep.objid = seq.oid AND dep.deptype = 'a'
            JOIN pg_class tbl        ON tbl.oid = dep.refobjid
            JOIN pg_namespace tbl_ns ON tbl_ns.oid = tbl.relnamespace
            WHERE seq_ns.nspname = 'public'
              AND tbl_ns.nspname = 'public'
              AND seq.relkind = 'S'
              AND tbl.relname = v_new_name
        LOOP
            IF s.seq_name NOT LIKE v_prefix || '%' THEN
                IF EXISTS (
                    SELECT 1
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public'
                      AND c.relkind = 'S'
                      AND c.relname = v_prefix || s.seq_name
                ) THEN
                    RAISE EXCEPTION 'Target sequence name already exists: public.%', v_prefix || s.seq_name;
                END IF;

                EXECUTE format('ALTER SEQUENCE public.%I RENAME TO %I', s.seq_name, v_prefix || s.seq_name);
            END IF;
        END LOOP;
    END LOOP;
END
$$;
'@

    $sql = $renameSqlTemplate.Replace('__PREFIX__', $escapedPrefix).Replace('__TABLES__', $tableArraySql)
    Set-Content -Path $OutputPath -Value $sql -Encoding UTF8
}

# =========================================================
# VALIDATION
# =========================================================
Test-CommandExists -CommandName 'pg_dump'
Test-CommandExists -CommandName 'psql'

if ([string]::IsNullOrWhiteSpace($TablePrefix)) {
    throw "TablePrefix must not be empty."
}

if ($TablePrefix -notmatch '^[A-Za-z_][A-Za-z0-9_]*$') {
    throw "Invalid prefix '$TablePrefix'. Only letters, numbers, and underscores are allowed, and it must start with a letter or underscore."
}

New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null

$SchemaFile = Join-Path $WorkDir 'public_schema.sql'
$DataFile   = Join-Path $WorkDir 'public_data.sql'
$RenameFile = Join-Path $WorkDir 'rename_with_prefix.sql'

Write-Host "Working directory: $WorkDir" -ForegroundColor Green

if ($TablePrefix -cmatch '[A-Z]') {
    Write-Host "Warning: uppercase letters in TablePrefix will create quoted PostgreSQL identifiers." -ForegroundColor Yellow
}

# =========================================================
# DETECT vector EXTENSION SCHEMA IN SOURCE AND TARGET
# =========================================================
$SourceVectorSchema = Get-ExtensionSchema -DbUrl $OldDbUrl -ExtensionName 'vector'
$TargetVectorSchema = Get-ExtensionSchema -DbUrl $NewDbUrl -ExtensionName 'vector'

if ([string]::IsNullOrWhiteSpace($SourceVectorSchema)) {
    $SourceVectorSchemaText = '<not installed>'
}
else {
    $SourceVectorSchemaText = $SourceVectorSchema
}

if ([string]::IsNullOrWhiteSpace($TargetVectorSchema)) {
    $TargetVectorSchemaText = '<not installed>'
}
else {
    $TargetVectorSchemaText = $TargetVectorSchema
}

Write-Host ""
Write-Host "Source vector schema: $SourceVectorSchemaText" -ForegroundColor Yellow
Write-Host "Target vector schema: $TargetVectorSchemaText" -ForegroundColor Yellow

# If vector exists in source but not in target, create it in the same schema
# so dumped references like public.vector(...) continue to work.
if ($SourceVectorSchema -and -not $TargetVectorSchema) {
    $quotedVectorSchema = Quote-SqlIdentifier $SourceVectorSchema

    if ($SourceVectorSchema -eq 'extensions') {
        Run-ExternalCommand -FilePath 'psql' -Arguments @(
            '--dbname', $NewDbUrl,
            '--single-transaction',
            '--variable', 'ON_ERROR_STOP=1',
            '-c', 'CREATE SCHEMA IF NOT EXISTS extensions; CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA extensions;'
        )
    }
    else {
        Run-ExternalCommand -FilePath 'psql' -Arguments @(
            '--dbname', $NewDbUrl,
            '--single-transaction',
            '--variable', 'ON_ERROR_STOP=1',
            '-c', "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA $quotedVectorSchema;"
        )
    }

    $TargetVectorSchema = Get-ExtensionSchema -DbUrl $NewDbUrl -ExtensionName 'vector'
}

# =========================================================
# LOAD SOURCE/TARGET TABLES
# =========================================================
$SourceTables = Get-PublicTables -DbUrl $OldDbUrl
$TargetTables = Get-PublicTables -DbUrl $NewDbUrl

if ($SourceTables.Count -eq 0) {
    throw "No tables were found in schema 'public' in the source project."
}

Write-Host ""
Write-Host "Found source tables:" -ForegroundColor Yellow
$SourceTables | ForEach-Object { Write-Host " - $_" }

$TargetTableSet = @{}
foreach ($tbl in $TargetTables) {
    $TargetTableSet[$tbl] = $true
}

$RestoreConflicts = @(
    $SourceTables | Where-Object { $TargetTableSet.ContainsKey($_) }
)

$MissingUnprefixedTables = @(
    $SourceTables | Where-Object { -not $TargetTableSet.ContainsKey($_) }
)

$PrefixedConflicts = @(
    $SourceTables | ForEach-Object { $TablePrefix + $_ } | Where-Object { $TargetTableSet.ContainsKey($_) }
)

if ($PrefixedConflicts.Count -gt 0) {
    throw @"
The target project already contains tables with the intended prefixed names.
The rename step would fail.

Conflicting tables:
$($PrefixedConflicts -join "`n")
"@
}

# =========================================================
# PATH A: ALL source tables already exist unprefixed in target
# Assume a previous partial restore and rename only.
# =========================================================
if (($RestoreConflicts.Count -gt 0) -and ($MissingUnprefixedTables.Count -eq 0)) {
    if (-not $ResumeFromExistingTargetTables) {
        throw @"
The target project already contains all unprefixed tables with the same names.
The restore would fail.

Conflicting tables:
$($RestoreConflicts -join "`n")
"@
    }

    Write-Host ""
    Write-Host "Detected existing unprefixed target tables. Skipping dump/restore and renaming them only." -ForegroundColor Yellow

    Write-RenameSqlFile -TableNames $RestoreConflicts -Prefix $TablePrefix -OutputPath $RenameFile

    Run-ExternalCommand -FilePath 'psql' -Arguments @(
        '--single-transaction',
        '--variable', 'ON_ERROR_STOP=1',
        '--file', $RenameFile,
        '--dbname', $NewDbUrl
    )

    Write-Host ""
    Write-Host "Done." -ForegroundColor Green
    Write-Host "Existing target tables were renamed with prefix '$TablePrefix'." -ForegroundColor Green
    Write-Host "Files are located here: $WorkDir" -ForegroundColor Green
    return
}

# =========================================================
# PATH A2: PARTIAL target state
# Some source tables already exist unprefixed, others are missing.
# That is ambiguous, so stop here.
# =========================================================
if (($RestoreConflicts.Count -gt 0) -and ($MissingUnprefixedTables.Count -gt 0)) {
    throw @"
The target project is in a partial state.
Some source tables already exist unprefixed, but others are missing.

Existing unprefixed tables:
$($RestoreConflicts -join "`n")

Missing unprefixed tables:
$($MissingUnprefixedTables -join "`n")

Please clean the target project first, or rename/drop the partially restored tables.
"@
}

# =========================================================
# PATH B: normal fresh transfer
# =========================================================
Run-ExternalCommand -FilePath 'pg_dump' -Arguments @(
    '--dbname', $OldDbUrl,
    '--schema=public',
    '--schema-only',
    '--no-owner',
    '--no-privileges',
    '--file', $SchemaFile
)

$schemaContent = Get-Content -Raw -Path $SchemaFile

$schemaContent = [regex]::Replace(
    $schemaContent,
    '(?m)^\s*CREATE SCHEMA public;\r?\n?',
    ''
)

$schemaContent = [regex]::Replace(
    $schemaContent,
    '(?m)^\s*ALTER SCHEMA public OWNER TO .*?;\r?\n?',
    ''
)

if ($SourceVectorSchema -and $TargetVectorSchema -and $SourceVectorSchema -ne $TargetVectorSchema) {
    $vectorTokens = @(
        'vector',
        'halfvec',
        'sparsevec',
        'vector_l2_ops',
        'vector_ip_ops',
        'vector_cosine_ops',
        'vector_l1_ops',
        'halfvec_l2_ops',
        'halfvec_ip_ops',
        'halfvec_cosine_ops',
        'halfvec_l1_ops',
        'sparsevec_l2_ops',
        'sparsevec_ip_ops',
        'sparsevec_cosine_ops',
        'sparsevec_l1_ops',
        'bit_hamming_ops',
        'bit_jaccard_ops'
    )

    foreach ($token in $vectorTokens) {
        $schemaContent = $schemaContent.Replace("$SourceVectorSchema.$token", "$TargetVectorSchema.$token")
        $schemaContent = $schemaContent.Replace('"' + $SourceVectorSchema + '"' + ".$token", "$TargetVectorSchema.$token")
    }
}

Set-Content -Path $SchemaFile -Value $schemaContent -Encoding UTF8

Run-ExternalCommand -FilePath 'pg_dump' -Arguments @(
    '--dbname', $OldDbUrl,
    '--schema=public',
    '--data-only',
    '--no-owner',
    '--no-privileges',
    '--file', $DataFile
)

Run-ExternalCommand -FilePath 'psql' -Arguments @(
    '--single-transaction',
    '--variable', 'ON_ERROR_STOP=1',
    '--file', $SchemaFile,
    '--command', 'SET session_replication_role = replica',
    '--file', $DataFile,
    '--dbname', $NewDbUrl
)

Write-RenameSqlFile -TableNames $SourceTables -Prefix $TablePrefix -OutputPath $RenameFile

Run-ExternalCommand -FilePath 'psql' -Arguments @(
    '--single-transaction',
    '--variable', 'ON_ERROR_STOP=1',
    '--file', $RenameFile,
    '--dbname', $NewDbUrl
)

Write-Host ""
Write-Host "Done." -ForegroundColor Green
Write-Host "The public schema has been transferred." -ForegroundColor Green
Write-Host "Imported tables have been renamed with prefix '$TablePrefix'." -ForegroundColor Green
Write-Host "Files are located here: $WorkDir" -ForegroundColor Green
