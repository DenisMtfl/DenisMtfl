# rename_existing_supabase_tables.ps1
# Renames already existing tables in the target database by applying a prefix.
# This is for the current situation where the tables already exist in the target.

$ErrorActionPreference = "Stop"

# =========================================================
# CONFIGURE HERE
# =========================================================
$TargetDbUrl = 'postgresql://postgres.wbtsjsongmedoqvlwwwv:Cafeaffe99%21%21%24%24@aws-0-eu-central-1.pooler.supabase.com:5432/postgres'

# Prefer lowercase to avoid quoted PostgreSQL identifiers
$TablePrefix = 'esel_'

$TablesToRename = @(
    'eselembeddings',
    'eselsbruecken',
    'eselsbruecken_merksprueche',
    'favorites',
    'merksprueche',
    'user_ratings'
)

$WorkDir = Join-Path $PSScriptRoot 'supabase-db-transfer'
$RenameFile = Join-Path $WorkDir 'rename_existing_tables.sql'

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

# =========================================================
# VALIDATION
# =========================================================
Test-CommandExists -CommandName 'psql'

if ([string]::IsNullOrWhiteSpace($TablePrefix)) {
    throw "TablePrefix must not be empty."
}

if ($TablePrefix -notmatch '^[A-Za-z_][A-Za-z0-9_]*$') {
    throw "Invalid prefix '$TablePrefix'. Only letters, numbers, and underscores are allowed, and it must start with a letter or underscore."
}

if ($TablesToRename.Count -eq 0) {
    throw "TablesToRename must not be empty."
}

New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null

if ($TablePrefix -cmatch '[A-Z]') {
    Write-Host "Warning: uppercase letters in TablePrefix will create quoted PostgreSQL identifiers." -ForegroundColor Yellow
}

# =========================================================
# READ EXISTING TABLES FROM TARGET
# =========================================================
$PublicTablesQuery = @"
SELECT c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind IN ('r','p')
ORDER BY c.relname;
"@

$TargetTablesRaw = Run-ExternalCommandCapture -FilePath 'psql' -Arguments @(
    '--dbname', $TargetDbUrl,
    '-At',
    '-c', $PublicTablesQuery
)

$TargetTables = @(
    $TargetTablesRaw |
    ForEach-Object { "$_".Trim() } |
    Where-Object { $_ -ne '' }
)

$TargetTableSet = @{}
foreach ($tbl in $TargetTables) {
    $TargetTableSet[$tbl] = $true
}

# Make sure all source tables exist unprefixed
$MissingTables = @(
    $TablesToRename | Where-Object { -not $TargetTableSet.ContainsKey($_) }
)

if ($MissingTables.Count -gt 0) {
    throw @"
Some expected unprefixed tables do not exist in the target database.

Missing tables:
$($MissingTables -join "`n")
"@
}

# Make sure prefixed names do not already exist
$PrefixedConflicts = @(
    $TablesToRename | ForEach-Object { $TablePrefix + $_ } | Where-Object { $TargetTableSet.ContainsKey($_) }
)

if ($PrefixedConflicts.Count -gt 0) {
    throw @"
Some prefixed target table names already exist.

Conflicting prefixed tables:
$($PrefixedConflicts -join "`n")
"@
}

# =========================================================
# GENERATE RENAME SQL
# =========================================================
$EscapedPrefix = $TablePrefix -replace "'", "''"
$TableArraySql = "ARRAY[" + (($TablesToRename | ForEach-Object { Quote-SqlLiteral $_ }) -join ", ") + "]::text[]"

$RenameSql = @"
DO \$\$
DECLARE
    v_prefix   text   := '$EscapedPrefix';
    v_tables   text[] := $TableArraySql;
    v_old_name text;
    v_new_name text;
    s record;
BEGIN
    FOREACH v_old_name IN ARRAY v_tables LOOP
        v_new_name := v_prefix || v_old_name;

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
                EXECUTE format('ALTER SEQUENCE public.%I RENAME TO %I', s.seq_name, v_prefix || s.seq_name);
            END IF;
        END LOOP;
    END LOOP;
END
\$\$;
"@

# Replace escaped dollar quotes with real PostgreSQL dollar quotes
$RenameSql = $RenameSql.Replace('\$\$', '$$')

Set-Content -Path $RenameFile -Value $RenameSql -Encoding UTF8

# =========================================================
# EXECUTE RENAME
# =========================================================
Run-ExternalCommand -FilePath 'psql' -Arguments @(
    '--single-transaction',
    '--variable', 'ON_ERROR_STOP=1',
    '--file', $RenameFile,
    '--dbname', $TargetDbUrl
)

Write-Host ""
Write-Host "Done." -ForegroundColor Green
Write-Host "Existing tables were renamed with prefix '$TablePrefix'." -ForegroundColor Green
Write-Host "SQL file: $RenameFile" -ForegroundColor Green
