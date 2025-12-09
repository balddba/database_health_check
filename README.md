# database_health_check

Oracle database health validation utility for comprehensive health checks against configured Oracle databases.

## Overview

This tool automatically validates Oracle database configuration and runtime parameters against a set of baseline rules and best practices. It supports multiple databases, database-specific rule overrides, and generates detailed health reports in HTML format.

## Features

- **Automated Health Checks**: Validates memory configuration, feature settings, database objects, and more
- **Multi-Database Support**: Monitor multiple Oracle databases simultaneously
- **Customizable Rules**: Define validation rules with database-specific overrides
- **HTML Reports**: Generate beautiful HTML reports with pass/fail summaries
- **Connection Pooling**: Efficient connection management with pooling
- **Profile Validation**: Validate password validation functions across profiles
- **Scheduler Job Monitoring**: Track scheduler jobs and log retention settings
- **Debug Logging**: Comprehensive debug output for troubleshooting

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd database_health_check

# Install dependencies using uv
uv sync
```

### Configuration

⚠️ **SECURITY WARNING**: Never commit actual database credentials to version control. The `databases.yaml` file is intentionally ignored by git.

1. **Configure Databases**

   Create/edit `database_health_checks/databases.yaml`:

   ```yaml
   databases:
     PROD:
       hostname: db-prod.example.com
       port: 1521
       service_name: PRODDB
       username: health_check_user
       password: ${ORACLE_PROD_PASSWORD}
       auth_mode: default
     
     DEV:
       hostname: db-dev.example.com
       port: 1521
       service_name: DEVDB
       username: health_check_user
       password: ${ORACLE_DEV_PASSWORD}
       auth_mode: default
   ```

   **Important**: Always use environment variables for passwords (syntax: `${VAR_NAME}`). Never hardcode passwords in `databases.yaml`.

2. **Set Environment Variables**

   ```bash
   export ORACLE_PROD_PASSWORD="your_prod_password"
   export ORACLE_DEV_PASSWORD="your_dev_password"
   ```

   For production, use a secure secrets management system (e.g., HashiCorp Vault, AWS Secrets Manager, etc.)

3. **Configure Validation Rules** (optional)

   Modify `database_health_checks/validation_rules.yaml` to adjust thresholds and enable/disable checks per database.

### Running Health Checks

```bash
# Run all checks for all configured databases
python3 -m database_health_checks.oracle_health_check

# Enable debug output for troubleshooting
python3 -m database_health_checks.oracle_health_check --debug
```

## Configuration Files

### databases.yaml

Stores Oracle database connection information.

**Fields:**
- `hostname`: Database server hostname or IP
- `port`: Listener port (default: 1521)
- `service_name`: Oracle service name
- `username`: Database user
- `password`: User password (use environment variables for security)
- `auth_mode`: Authentication mode (default, sysdba, sysoper)

### validation_rules.yaml

Defines health check thresholds and rules with support for database-specific overrides.

**Sections:**
- **Memory Configuration**: SGA_TARGET, PGA settings, memory targets
- **Feature Configuration**: Archivelog, flashback, auditing settings
- **Database Objects**: Database links, scheduler jobs, redo logs
- **Password Validation**: Profile-based password validation functions

Example override for FREE tier database:

```yaml
validation_rules:
  overrides:
    FREE:
      sga_target_min_gb: 1
      memory_target_required: false
      flashback_enabled: false
```

## Health Checks Supported

### Memory Configuration
- SGA_TARGET minimum size
- SGA_MAX_SIZE requirement
- PGA_AGGREGATE_TARGET and PGA_AGGREGATE_LIMIT
- MEMORY_TARGET settings
- OPTIMIZER_MODE configuration
- Process and session limits
- Open cursors limits

### Feature Configuration
- Archivelog mode
- Flashback database
- Force logging
- Unified auditing
- Management packs

### Database Objects
- Database links count
- Scheduler jobs configuration
- Job queue processes
- Control file multiplexing
- ASM datafile placement
- Redo log sizing

### Profile Validation
- Password validation functions
- Profile-based security settings

## Project Structure

```
database_health_check/
├── database_health_checks/
│   ├── checks/                    # Individual check implementations
│   ├── models/                    # Data models (CheckResult, etc.)
│   ├── templates/                 # HTML report templates
│   ├── reports/                   # Generated HTML reports
│   ├── oracle_health_check.py     # Main health check manager
│   ├── check_registry.py          # Check registry
│   ├── inventory.py               # Database inventory loader
│   ├── validation_manager.py      # Validation rules manager
│   ├── databases.yaml             # Database configurations
│   └── validation_rules.yaml      # Validation rules
├── INVENTORY_SETUP.md             # Inventory configuration guide
├── README.md                      # This file
└── pyproject.toml                 # Project dependencies
```

## Output

### Console Output

The tool prints:
- Health check results summary
- Pass/fail status for each check
- Database-level and overall scoring
- Profile validation reports
- Scheduler job monitoring details

### HTML Reports

Generated in `database_health_checks/reports/`:
- Individual reports per database
- Report index page
- Summary scores and color-coded status
- Detailed check results by category
- Password validation details
- Scheduler job information

## Development

### Running Tests

```bash
# Run all tests
uv run pytest

# Run specific test suite
uv run pytest tests/check_registry

# Run with coverage
uv run pytest --cov=database_health_checks
```

### Code Quality

```bash
# Format code
uv run black .

# Lint with ruff
uv run ruff check .

# Fix linting issues
uv run ruff check --fix .
```

### Adding New Checks

1. Create a new check file in `database_health_checks/checks/`
2. Implement the check class with an `execute()` method
3. Register the check in `check_registry.py`
4. Add corresponding rules in `validation_rules.yaml`

## Troubleshooting

### Connection Issues

**Error**: `Failed to create connection pool`

- Verify hostname, port, and service_name in databases.yaml
- Check that database listener is running
- Confirm network connectivity to database server

### Environment Variable Not Found

**Error**: `Environment variable X not set`

- Ensure all referenced environment variables are exported
- Check variable names match exactly (case-sensitive)

### Inventory File Not Found

**Error**: `Database inventory file not found`

- Ensure `databases.yaml` exists in `database_health_checks/`
- Or pass custom path to Inventory constructor

### No Databases Found

**Error**: `No databases configured in inventory file`

- Verify `databases.yaml` has a `databases:` section
- Add at least one database entry

## Database Support

Currently supports:
- Oracle Database 19c+
- Oracle Database 21c
- Oracle Database 23c
- Oracle Free Tier

## Dependencies

- `oracledb>=3.4.0` - Oracle database driver
- `pydantic>=2.12.4` - Data validation
- `pyyaml>=6.0.3` - YAML configuration
- `jinja2>=3.0.0` - HTML templating
- `loguru>=0.7.0` - Logging
- `tabulate>=0.9.0` - Table formatting
- `fabric>=3.2.2` - Remote execution
- `textual>=6.6.0` - Terminal UI

## License

See LICENSE file for details.

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## Support

For issues, questions, or suggestions:
- Review debug output: run with `--debug` flag
- Check Oracle database logs for connection issues

## Changelog

### Version 0.1.0
- Initial release
- YAML-based database inventory
- Comprehensive health checks
- HTML report generation
- Password validation monitoring
- Scheduler job tracking
