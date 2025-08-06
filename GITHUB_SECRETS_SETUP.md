# GitHub Secrets Configuration

This document outlines the required GitHub secrets for the weekly dbt run workflow.

## Required Secrets

### Database Connection (dbt profile)
- `DBT_HOST` - PostgreSQL database host
- `DBT_USER` - Database username
- `DBT_PASSWORD` - Database password
- `DBT_PORT` - Database port (typically 5432)
- `DBT_DBNAME` - Database name
- `DBT_SCHEMA` - Default schema for dbt operations

### OpenAI API Configuration
- `OPENAI_API_KEY` - Your OpenAI API key with access to o3-mini model

### Email Configuration
- `EMAIL_HOST` - SMTP server host (e.g., smtp.gmail.com)
- `EMAIL_PORT` - SMTP server port (typically 587 for TLS)
- `EMAIL_USER` - SMTP username/email address
- `EMAIL_PASSWORD` - SMTP password or app-specific password
- `RECIPIENT_EMAIL` - Email address to receive the weekly reports

## Setup Instructions

1. Go to your GitHub repository settings
2. Navigate to "Secrets and variables" > "Actions"
3. Click "New repository secret" for each secret above
4. Add the secret name and value

## Email Provider Examples

### Gmail
- `EMAIL_HOST`: smtp.gmail.com
- `EMAIL_PORT`: 587
- `EMAIL_USER`: your-email@gmail.com
- `EMAIL_PASSWORD`: Use an app-specific password (not your regular password)

### Outlook/Hotmail
- `EMAIL_HOST`: smtp-mail.outlook.com
- `EMAIL_PORT`: 587
- `EMAIL_USER`: your-email@outlook.com
- `EMAIL_PASSWORD`: Your account password or app password

### SendGrid
- `EMAIL_HOST`: smtp.sendgrid.net
- `EMAIL_PORT`: 587
- `EMAIL_USER`: apikey
- `EMAIL_PASSWORD`: Your SendGrid API key

## Security Notes

- Never commit secrets to your repository
- Use app-specific passwords when available
- Consider using dedicated service accounts for database access
- Regularly rotate your API keys and passwords
- Limit database user permissions to only what's needed for dbt operations

## Workflow Schedule

The workflow is configured to run:
- **Scheduled**: Every Monday at 9:00 AM UTC
- **Manual**: Can be triggered manually from the GitHub Actions tab

## Troubleshooting

If the workflow fails:

1. Check that all secrets are properly set
2. Verify database connectivity and permissions
3. Ensure OpenAI API key has sufficient credits and access to o3-mini
4. Check email configuration and authentication
5. Review the workflow logs in GitHub Actions for specific error messages