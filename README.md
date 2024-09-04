# prismafy

  prismafy - It’s an open-source tool developed in Python to analyze metadata 
  for cloud-native data platforms. It provides charts and insights enabling you 
  to easily detect and fix problems faster. Originally developed by Deiby Gomez.

  Documentation: https://drive.google.com/file/d/1LiiGsA8ezzYNvHkLIe9uA75YS_pWQkST/view?usp=sharing

## License

  prismafy - It’s an open-source tool developed in Python to analyze metadata 
  for cloud-native data platforms. It provides charts and insights enabling you 
  to easily detect and fix problems faster. Originally developed by Deiby Gomez.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.


## Important notes about Prismafy

- It does not write anything to your database.
- It does not store your data externally.
- It does not access your business data.

## Required privileges

  Prismafy requires access to SNOWFLAKE.ACCOUNT_USAGE schema, which by default can be accessed only by ACCOUNTADMIN role.

  If you do not want to run Prismafy with ACCOUNTADMIN role, the following are the minimum steps to run Prismafy:

  create role prismafy_role;
  create warehouse prismafy_wh   warehouse_size = xsmall   warehouse_type = standard   auto_suspend = 60   auto_resume = true   initially_suspended = true;
  create  user prismafy password = '*****' default_role = prismafy_role   default_warehouse = prismafy_wh;
  grant role prismafy_role to user prismafy; 
  grant usage on warehouse prismafy_wh   to role prismafy_role;
  grant imported privileges on database snowflake to role prismafy_role;
  grant monitor on  account to role prismafy_role; --Needed for Execution Plans

## Authentication Methods

  Prismafy supports 3 authentication methods:

  - External Authentication
  - User & Password
  - User & Password with MFA

## Scope

  Prismafy includes the following Sections, by default Prismafy generates all the sections, however you can specify a custom scope. 

  - Section A: Computing

    Credits Consumption per warehouse for the history, last month and last week
    Credits Consumption Details for each warehouse
    Daily Credits consumption per Snowflake Service

  - Section B: Storage

    Top Tables by Active Bytes
    Top Tables by Time Travel Bytes
    Top Tables by Fail Safe Bytes
    Top Tables by Retained for clone Bytes
    Top Databases by Used Storage
    Storage Details per Database

  - Section C: Credits

    Credits Consumption per warehouse for the history, last month and last week
    Credits Consumption Details for each warehouse
    Daily Credits consumption per Snowflake Service

  - Section D: Performance

    Top Query per several metrics, for last month and last week. 
    Top Tables per reclustering  
    Top Tables with poor pruning
    For the top 10 Query:
      Bytes Details 
      Calls Details (how frequently is called)
      Time Details
      Rows Details 
      Historical Execution Plans
      Changes on Execution Plans 
      Growth over each execution
      Pruning Efficiency for Tables Used by the Query
      Accessed Objects by the Query

  - Section E: Security

    Failed Logins
    New Users Logins in last month and last week
    Unfrequented Users with Logins
    Users with Accountadmin and Securityadmin privileges
    Recent changes on Network policies, network rules, masking policies, row access polidies
    Recent Password changes
    Sessions per authentication method
    Users with top Logins 
    Changes on Client Drivers used by Users
    Changes on IPs for Logins 

  - Section F: Data Transfer

    Top Clouds used for data transfer
    Historical behavior for data transfer per cloud
    Replication Usage per database
    External Functions

  - Section G: Maintenance

    Less Accessed Objects
    Users with no sessions in the last 6 months
    Users with no sessions in the last 3 months
    Snowflake Tasks in Status that need attention
    Snowpipes in Status that need attention
    *Non Default parameters for Account, Databases and Warehouses
    Warehouses with no activity in the last 3 months
    Warehouses with no activity in the last month
    Historical Behavior per SQL Operation Type
    Historical Behavior per SQL Operation Type per database
    Resource Monitors

  - Section H: DBT

    Models information (DBT must be configured with Query Comments https://docs.getdbt.com/reference/project-configs/query-comment)
    Top models for last month
    Top models for last week


## Fine-Grained Scope

You can also run Prismafy for a custom scope:

  - An specific Section (Argument -s)
  - An specific Warehouse (Argument -aw)
  - An specific query_parameterized_hash (Argument -aq)

## Data History

  By default prismafy scans 6 months of your data history, however you can specify a custom number of months to scan.

## How To Run Prismafy?

  - Download Prismafy zip file from github (https://github.com/prismafy/prismafy)
  - Execute Prismafy:
    python prismafy.py -h
  - Open the index report in the location: prismafy-reports/prismafy-{date}/prismafy_index.html

## Examples

  All sections for last 6 months of history, using MFA, and hiding password:

  python prismafy.py -d snowflake -t username_password_mfa -a abc.us-east-2.aws -w warehousename -u user1 –k 171297 -m 6 -r accountadmin

  Only Section D (Performance) for last 12 months of history, using external browser authentication:

  python prismafy.py -d snowflake -t externalbrowser -a abc.us-east-2.aws -w warehousename -u user1 -m 12 -r accountadmin –s D

  Analyze only a specific Query (query_parameterized_hash) for last month of history:

  python prismafy.py -d snowflake -t password -a abc.us-east-2.aws -w warehousename -u user1 -p welcome1 -m 1 -r accountadmin –aq query_parameterized_hash

  Analyze only a specific Warehouse (warehouse_name) for last 3 months of history:

  python prismafy.py -d snowflake -t password -a abc.us-east-2.aws -w warehousetoconnect -u user1 -p welcome1 -m 3 -r accountadmin –aw warehouse_to_analyze

## Help

python prismafy.py -h

## Thank you!

Feedback, report a bug, enhancements and new features, questions: prismafy@gmail.com

