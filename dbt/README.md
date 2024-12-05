# Session Analytics dbt Project

This dbt project transforms raw session data into analytics-ready tables for user session analysis.

## Project Structure

```
models/
├── input/
│   ├── user_session_channel.sql     # Input model for user session data
│   └── session_timestamp.sql        # Input model for session timestamps
├── output/
│   └── session_summary.sql          # Final analytics model
├── sources.yml                      # Source definitions
└── schema.yml                       # Model definitions and tests
```

## Models

1. **Input Models** (`models/input/`):
   - `user_session_channel`: Cleans raw user session and channel data
   - `session_timestamp`: Cleans raw timestamp data

2. **Output Models** (`models/output/`):
   - `session_summary`: Combines and transforms session data with timestamps

3. **Configuration Files**:
   - `sources.yml`: Defines raw data sources from Snowflake
   - `schema.yml`: Contains model definitions and data quality tests

## Data Tests
The project includes essential data quality tests:
1. Unique sessionId
2. Not null checks for critical fields
3. Accepted values for channel types

## Getting Started

1. Set up your Snowflake credentials as environment variables:
```bash
export DBT_ACCOUNT=your_account
export DBT_DATABASE=dev
export DBT_PASSWORD=your_password
export DBT_ROLE=your_role
export DBT_TYPE=snowflake
export DBT_USER=your_username
export DBT_WAREHOUSE=your_warehouse
```

2. Run the models:
```bash
dbt run
```

3. Test the models:
```bash
dbt test
```

## Project Dependencies
- dbt-snowflake
- Snowflake account with appropriate permissions

### Resources:
- [dbt Docs](https://docs.getdbt.com/docs/introduction)
- [dbt Discourse](https://discourse.getdbt.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
