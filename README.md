# CRM Visit Scheduler

This application automates the scheduling of customer visits for sales teams in Dynamics 365 CRM.

## Features

- Automated visit scheduling based on business rules
- Weighted distribution of visits by customer importance
- Holiday and weekend awareness
- Compliance with visit frequency requirements
- Bulk processing of sales users

## Business Logic

1. *User Processing*:
   - Fetches sales users with specific role filters
   - Processes each user's territory and accounts

2. *Account Weighting*:
   - Calculates importance weight for each account based on:
     - Sales performance (YTD)
     - Growth/de-growth status
     - Customer type (Dealer/Retailer)
   - Applies different weighting rules based on user role

3. *Visit Calculation*:
   - Determines required visits from compliance matrix
   - Calculates working days (excluding weekends/holidays)
   - Distributes visits across the month

4. *Scheduling*:
   - Creates visit header for the month
   - Generates daily visit plans
   - Balances visits across routes
   - Handles remaining visits at month end

## Security Requirements

- Requires CRM connection with appropriate privileges
- Never stores credentials in source code
- Uses secure storage for connection strings
- Implements proper error handling

## Configuration

Set environment variable:

```bash
# Format:
# CRM_CONNECTION_STRING="AuthType=OAuth;Url=[ORG_URL];Username=[USER];Password=[PWD];ClientId=[CLIENT_ID];RedirectUri=[REDIRECT_URI]"

export CRM_CONNECTION_STRING="AuthType=OAuth;Url=https://yourorg.crm.dynamics.com;..."
