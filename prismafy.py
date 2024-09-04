"""
*******************************************************************************  
*** prismafy - Tool to analyze metadata for cloud native data platforms.    ***
*** prismafy_copyright (C) 2024  Deiby Gomez                                ***
***                                                                         ***
*** This program is free software: you can redistribute it and/or modify    ***
*** it under the terms of the GNU General Public License as published by    ***
*** the Free Software Foundation, either version 3 of the License, or       ***
*** (at your option) any later version.                                     ***
***                                                                         *** 
*** This program is distributed in the hope that it will be useful,         ***
*** but WITHOUT ANY WARRANTY; without even the implied warranty of          ***
*** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           ***
*** GNU General Public License for more details.                            ***
***                                                                         ***
*** You should have received a copy of the GNU General Public License       ***
*** along with this program.  If not, see <http://www.gnu.org/licenses/>.   ***
*******************************************************************************

*******************************************************************************
*** Tool:            prismafy                                               ***
*** Version:         1.0                                                    ***
*** Project:         https://github.com/prismafy/prismafy                   ***
*** Developer:       Deiby Gomez                                            ***
*** Date:            08/2024                                                ***
*******************************************************************************
"""

from base64 import b64encode
import argparse
import os
import snowflake.connector
from datetime import datetime, timedelta
import shutil
import getpass

__version__="""Copyright (C) 2024 - prismafy
version: 1.0 - version date: 08/2024 -
visit the project: https://github.com/prismafy/prismafy"""

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('-d',  '--databasetype' ,help="Type of database.", choices=['snowflake', 'databricks'],type=str )
parser.add_argument('-t',  '--authenticator' ,help="How to authenticate in Snowflake. Password is not needed for externalbrowser authentication.",  choices=['externalbrowser', 'password','username_password_mfa'], type=str)
parser.add_argument('-a',  '--account' ,help="Specify Snowflake account name.", type=str)
parser.add_argument('-w',  '--warehouse' ,help="Specify the warehouse name to use.", type=str)
parser.add_argument('-u',  '--username' ,help="Username to Login", type=str)
parser.add_argument('-p',  '--password' ,help="Username's password. Password is not needed for externalbrowser authentication.",action='store', type=str)
parser.add_argument('-k',  '--token' ,help="Token for MFA provided by Duo App", type=str)
parser.add_argument('-r',  '--role' ,help="Role to use in the session.", type=str)
parser.add_argument('-m',  '--months' ,help="Months of data to scan in the history. Min=1 month, Max=24 months.", type=int, choices=range(1, 25), default=6 )
parser.add_argument('-s',  '--reportsections' ,help="Build one specific report section: A=Computing, B=Storage, C=Credits, D=Performance, E=Security, F=DataTransfer, G=Maintenance, H=DBT, Z=All",  choices=['A', 'B','C','D','E','F','G','H','Z'], type=str , default="Z")
parser.add_argument('-aq', '--analyzequery' ,help="Run report for an specific Query", type=str)
parser.add_argument('-aw', '--analyzewarehouse',help="Run report for an specific Warehouse", type=str)
parser.add_argument('-v', '--version',help="Returns Prismafy version", action='version',version='%(prog)s {version}'.format(version=__version__))
parser.add_argument('-h',  '--help' , action='help', default=argparse.SUPPRESS, help='Print all possible arguments.' )

args = parser.parse_args()

snowflake_conn =None
months_history= "-"+str(args.months)
report_time = datetime.now()
report_root_folder = "prismafy-"+report_time.strftime('%Y-%m-%d-%H-%M-%S')
report_formatted_time = "'"+report_time.strftime('%Y-%m-%d %H:%M:%S')+"'"
report_sections = {"A - Computing":{},"B - Storage":{},"C - Credits":{},"D - Performance":{},"E - Security":{},"F - Data Transfer":{},"G - Maintenance":{},"H - DBT":{}}
hash_plans ={}


html_table_header_index="""
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<!-- ********************************************************-->
<!-- *** Tool:      prismafy                              ***-->
<!-- *** Version:   1.0                                   ***-->
<!-- *** Project:   https://github.com/prismafy/prismafy  ***-->
<!-- ********************************************************-->
<head>
<meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
<style type="text/css">
        body {
            background-color: #e6f2f2;
            font-family: Arial, sans-serif; 
            color: #333333; 
        }
        h1 {
            color: #002d72; 
            font-family: Arial, sans-serif;
            font-size: 24px; 
            font-weight: bold; 
            border-bottom: none; 
            margin-top:0pt; 
            margin-bottom:0pt; 
            padding:0px 0px 0px 0px;
        }
        h2 {
            color: #009688; 
            font-family: Arial, sans-serif; 
            font-size: 16px; 
            font-weight: bold; 
            margin-top:0pt; 
            margin-bottom:0pt; 
            padding:0px 0px 0px 0px;
        }
       .tabla1, .tabla2 {
            width: 100%;
            border-collapse: collapse;
            empty-cells:show; 
            white-space:nowrap;
        }
        .tabla1 {
            border: 3px solid #00aaff; /* Borde de la primera tabla (verde azulado) */
        }
        .tabla2 {
            border: 3px solid #004d40; /* Borde de la segunda tabla (azul marino oscuro) */
        }

        .tabla1 th, .tabla2 th {
            background-color: #00aaff; 
            color: #ffffff; 
            padding-left:4px; 
            padding-right:4px; 
            padding-bottom:2px;      
            text-align: left;
            font-family: Arial, sans-serif; 
            font-size: 10px; 
            font-weight: bold; 
        }
        .tabla1 td, .tabla2 td {
            background-color: #b3e5cc; 
            color: #333333; 
            padding-left:4px; 
            padding-right:4px; 
            padding-bottom:2px;          
            font-family: Arial, sans-serif; 
            font-size: 10px; 
        }
        .tabla2 th {
            background-color: #004d40; 
        }
        .tabla2 td {
            background-color: #e0f7fa; 
        }
        div.google-chart {
            width:809px; height:500px;
        }
        .column { 
            flex: 50%;   padding: 10px;   height: 300px; 
        }
        .row { 
            display: flex; 
        }
        a {
            color: #00796b; /* Color del enlace normal (verde azulado) */
            text-decoration: underline; /* Subrayado para enlaces */
            font-family: Arial, sans-serif; /* Tipo de letra de los enlaces */
            font-size: 10px; /* Tamaño de la fuente de los enlaces */
            font-weight: bold;
            transition: color 0.3s; /* Transición suave para el color del enlace */
        }
        a:hover {
            color: #004d40; /* Color del enlace al pasar el cursor (verde azulado oscuro) */
        }
        a:visited {
            color: #9c27b0; /* Color del enlace visitado (púrpura) */
        }
        img {
        height: 30px;
        }
</style>
<body>
<div class="row">
<div class="column">
<h1> <img src="prismafy.png" alt="prismafy"> <img src="prismafy_font.png" ></h1>
"""

html_table_header="""
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<!-- ********************************************************-->
<!-- *** Tool:      prismafy                              ***-->
<!-- *** Version:   1.0                                   ***-->
<!-- *** Project:   https://github.com/prismafy/prismafy  ***-->
<!-- ********************************************************-->
<head>
<meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
<style type="text/css">
       body {
            background-color: #e6f2f2;
            font-family: Arial, sans-serif; 
            color: #333333; 
        }
        h1 {
            color: #002d72; 
            font-family: Arial, sans-serif;
            font-size: 24px; 
            font-weight: bold; 
            border-bottom: none; 
            margin-top:0pt; 
            margin-bottom:0pt; 
            padding:0px 0px 0px 0px;
        }
        h2 {
            color: #009688; 
            font-family: Arial, sans-serif; 
            font-size: 16px; 
            font-weight: bold; 
            margin-top:0pt; 
            margin-bottom:0pt; 
            padding:0px 0px 0px 0px;
        }
        h3 {
            color: #002d72; 
            font-family: "Times New Roman", Times, serif;
            font-size: 14px; 
            font-weight: normal; 
            margin-top:0pt; 
            margin-bottom:0pt; 
            padding:0px 0px 0px 0px;
        }
       .tabla1, .tabla2 {
            width: 100%;
            border-collapse: collapse;
            empty-cells:show; 
            white-space:nowrap;
        }
        .tabla1 {
            border: 3px solid #00aaff; /* Borde de la primera tabla (verde azulado) */
        }
        .tabla2 {
            border: 3px solid #004d40; /* Borde de la segunda tabla (azul marino oscuro) */
        }

        .tabla1 th, .tabla2 th {
            background-color: #00aaff; 
            color: #ffffff; 
            padding-left:4px; 
            padding-right:4px; 
            padding-bottom:2px;      
            text-align: left;
            font-family: Arial, sans-serif; 
            font-size: 10px; 
            font-weight: bold; 
        }
        .tabla1 td, .tabla2 td {
            background-color: #b3e5cc; 
            color: #333333; 
            padding-left:4px; 
            padding-right:4px; 
            padding-bottom:2px;          
            font-family: Arial, sans-serif; 
            font-size: 10px; 
            border: 1px solid #00aaff;
        }
        .cell_grow {
            width: 100%; 
        }
        .tabla2 th {
            background-color: #004d40; 
        }
        .tabla1 td {
            border: 1px solid #00aaff;
        }
        .tabla2 td {
            background-color: #e0f7fa; 
            border: 1px solid #b3e5cc;
        }
        div.google-chart {
            width:809px; height:500px;
        }
        .column { 
            flex: 50%;   padding: 10px;   height: 300px; 
        }
        .row { 
            display: flex; 
        }
        a {
            color: #00796b; /* Color del enlace normal (verde azulado) */
            text-decoration: underline; /* Subrayado para enlaces */
            font-family: Arial, sans-serif; /* Tipo de letra de los enlaces */
            font-size: 10px; /* Tamaño de la fuente de los enlaces */
            font-weight: bold;
            transition: color 0.3s; /* Transición suave para el color del enlace */
        }
        a:hover {
            color: #004d40; /* Color del enlace al pasar el cursor (verde azulado oscuro) */
        }
        a:visited {
            color: #9c27b0; /* Color del enlace visitado (púrpura) */
        }
        img {
        height: 30px;
        }
</style>
<body>
<img src="prismafy.png" alt="prismafy"> <img src="prismafy_font.png" >
<h3>Prismafy v1.0 - - https://github.com/prismafy/prismafy</h3>
<h3>Chart Creation Date: """+report_formatted_time+"""</h3>
"""

html_table_tail="""
</table>  </body> </html> 
"""
sql_header="""
--********************************************************
--*** Tool:      prismafy                              ***
--*** Version:   1.0                                   ***
--*** Project:   https://github.com/prismafy/prismafy  ***
--********************************************************
"""

html_header="""
<html>
<!-- ********************************************************-->
<!-- *** Tool:      prismafy                              ***-->
<!-- *** Version:   1.0                                   ***-->
<!-- *** Project:   https://github.com/prismafy/prismafy  ***-->
<!-- ********************************************************-->
<head>
<style type="text/css">
       body {
            background-color: #e6f2f2;
            font-family: Arial, sans-serif; 
            color: #333333; 
        }
        h1 {
            color: #002d72; 
            font-family: Arial, sans-serif; 
            font-size: 24px; 
            font-weight: bold; 
            border-bottom: none; 
            margin-top:0pt; 
            margin-bottom:0pt; 
            padding:0px 0px 0px 0px;
        }
        h2 {
            color: #009688; 
            font-family: Arial, sans-serif; 
            font-size: 16px; 
            font-weight: bold; 
            margin-top:0pt; 
            margin-bottom:0pt; 
            padding:0px 0px 0px 0px;
        }
        img {
        height: 30px;
        }
        .overlay {
            position: absolute;
            z-index: 1;
            margin-left: 400pt; 
            margin-top:10pt; 
        }
</style>
<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
<script type="text/javascript">
    google.charts.load('current', {'packages':['corechart']});
    google.charts.setOnLoadCallback(drawChart);
    function drawChart() {
    var data = [
"""

html_body1="""
];
data = data.map(function (row) {
return [
new Date(row[0]),
"""

html_body2="""
];
});
data =google.visualization.arrayToDataTable(data);

var ticks = [];
for (var i = 0; i < data.getNumberOfRows(); i++) {
 ticks.push(data.getValue(i, 0));
}

var options = {
"""

html_line_month_tail="""
    titleTextStyle: {
    fontSize: 14, bold: false},
    isStacked: true,
    chartArea:{left:90, top:75, bottom: 130, height: '90%', width: '70%' },
    backgroundColor: {fill: 'white', stroke: '#336699', strokeWidth: 1},			
    legend: {position: 'right', textStyle: {fontSize: 8}},
    hAxis: {textStyle: {fontSize: 12}, slantedText:true, slantedTextAngle:90, ticks: ticks,format: "MM/yyyy", showEveryText : 1},
    explorer: { actions: ['dragToZoom', 'rightClickToReset'],  axis: 'horizontal', keepInBounds: true, maxZoomIn: 0.01}
    };
        
    var chart = new google.visualization.LineChart(document.getElementById('curve_chart'));    
    chart.draw(data, options);
    }
    </script>
    </head>
    <body>
    	<div class="overlay" id="chart-overlay">    	
    	 <img src="prismafy.png" alt="prismafy"> <img src="prismafy_font.png" >
        </div>
        <div id="curve_chart" style="width: 1800px; height: 850px;"></div>
    </body>
</html>
"""

html_stepped_area_minute_tail="""
    interpolateNulls: true,
    titleTextStyle: {
    fontSize: 14, bold: false},
    isStacked: false,
    chartArea:{left:90, top:75, bottom: 130, height: '90%', width: '70%' },
    backgroundColor: {fill: 'white', stroke: '#336699', strokeWidth: 1},			
    legend: {position: 'right', textStyle: {fontSize: 8}},
    hAxis: {textStyle: {fontSize: 12}, slantedText:true, ticks: ticks, slantedTextAngle:90,  format: "MM/dd/yyyy HH:mm", showEveryText : 1},
    explorer: { actions: ['dragToZoom', 'rightClickToReset'],  axis: 'horizontal', keepInBounds: true, maxZoomIn: 0.001}
    };
        
    var chart = new google.visualization.SteppedAreaChart(document.getElementById('curve_chart'));
    chart.draw(data, options);
    }
    </script>
    </head>
    <body>
    	<div class="overlay" id="chart-overlay">    	
    	 <img src="prismafy.png" alt="prismafy"> <img src="prismafy_font.png" >
        </div>
        <div id="curve_chart" style="width: 1800px; height: 850px;"></div>
    </body>
</html
"""

html_line_hour_tail="""
    titleTextStyle: {
    fontSize: 14, bold: false},
    isStacked: true,
    chartArea:{left:90, top:75, bottom: 130, height: '90%', width: '70%' },
    backgroundColor: {fill: 'white', stroke: '#336699', strokeWidth: 1},			
    legend: {position: 'right', textStyle: {fontSize: 8}},
    hAxis: {textStyle: {fontSize: 12}, slantedText:true, slantedTextAngle:90, ticks: ticks,format: "MM/dd/yyyy hh:mm", showEveryText : 1},
    explorer: { actions: ['dragToZoom', 'rightClickToReset'],  axis: 'horizontal', keepInBounds: true, maxZoomIn: 0.01}
    };
        
    var chart = new google.visualization.LineChart(document.getElementById('curve_chart'));
    chart.draw(data, options);
    }
    </script>
    </head>
    <body>
    	<div class="overlay" id="chart-overlay">    	
    	 <img src="prismafy.png" alt="prismafy"> <img src="prismafy_font.png" >
        </div>
        <div id="curve_chart" style="width: 1800px; height: 850px;"></div>
    </body>
</html
"""

html_line_day_tail="""
    titleTextStyle: {
    fontSize: 14, bold: false},
    isStacked: true,
    chartArea:{left:90, top:75, bottom: 130, height: '90%', width: '70%' },
    backgroundColor: {fill: 'white', stroke: '#336699', strokeWidth: 1},			
    legend: {position: 'right', textStyle: {fontSize: 8}},
    hAxis: {textStyle: {fontSize: 12}, slantedText:true, slantedTextAngle:90, ticks: ticks,format: "MM/dd/yyyy", showEveryText : 1},
    explorer: { actions: ['dragToZoom', 'rightClickToReset'],  axis: 'horizontal', keepInBounds: true, maxZoomIn: 0.01}
    };
        
    var chart = new google.visualization.LineChart(document.getElementById('curve_chart'));
    chart.draw(data, options);
    }
    </script>
    </head>
    <body>
    	<div class="overlay" id="chart-overlay">    	
    	 <img src="prismafy.png" alt="prismafy"> <img src="prismafy_font.png" >
        </div>
        <div id="curve_chart" style="width: 1800px; height: 850px;"></div>
    </body>
</html
"""

html_bar_day_tail="""
	titleTextStyle: {fontSize: 14, bold: false},
	isStacked: 'percent',			
	chartArea:{left:90, top:75, bottom: 130, height: '90%', width: '70%' },
	backgroundColor: {fill: 'white', stroke: '#336699', strokeWidth: 1},			
	legend: {position: 'right', textStyle: {fontSize: 8}},
	hAxis: {textStyle: {fontSize: 12}, slantedText:true, slantedTextAngle:90, ticks: data.getDistinctValues(0),format: "MM/dd/yyyy", showEveryText : 1},
	explorer: { actions: ['dragToZoom', 'rightClickToReset'],  axis: 'horizontal', keepInBounds: true, maxZoomIn: 0.01}
    };

    var chart = new google.visualization.ColumnChart(document.getElementById('curve_chart'));
    
    function selectHandler() {
        var selectedItem = chart.getSelection()[0];
        if (selectedItem) {
        var value = data.getColumnLabel(selectedItem.column)
        alert('Selected Value: ' + value);
        }
    }

    google.visualization.events.addListener(chart, 'select', selectHandler);

    chart.draw(data, options);
    }
    </script>
    </head>
    <body>
    	<div class="overlay" id="chart-overlay">    	
    	 <img src="prismafy.png" alt="prismafy"> <img src="prismafy_font.png" >
        </div>
        <div id="curve_chart" style="width: 1800px; height: 850px;"></div>
    </body>
</html>
"""

html_bar_month_tail="""
	titleTextStyle: {fontSize: 14, bold: false},
	isStacked: 'percent',			
	chartArea:{left:90, top:75, bottom: 130, height: '90%', width: '70%' },
	backgroundColor: {fill: 'white', stroke: '#336699', strokeWidth: 1},			
	legend: {position: 'right', textStyle: {fontSize: 8}},
	hAxis: {textStyle: {fontSize: 12}, slantedText:true, slantedTextAngle:90, ticks: data.getDistinctValues(0),format: "MM/yyyy", showEveryText : 1},
	explorer: { actions: ['dragToZoom', 'rightClickToReset'],  axis: 'horizontal', keepInBounds: true, maxZoomIn: 0.01}
    };

    var chart = new google.visualization.ColumnChart(document.getElementById('curve_chart'));

    function selectHandler() {
        var selectedItem = chart.getSelection()[0];
        if (selectedItem) {
        var value = data.getColumnLabel(selectedItem.column)
        alert('Selected Value: ' + value);
        }
    }

    google.visualization.events.addListener(chart, 'select', selectHandler);

    chart.draw(data, options);
    }
    </script>
    </head>
    <body>
    	<div class="overlay" id="chart-overlay">    	
    	 <img src="prismafy.png" alt="prismafy"> <img src="prismafy_font.png" >
        </div>
        <div id="curve_chart" style="width: 1800px; height: 850px;"></div>
    </body>
</html>
"""
        
def main():
    global months_history
    months_history="-"+str(args.months)


    if args.databasetype=='snowflake':
        if args.authenticator is None:
            print ("authenticator was not provided.")                      
        else:
            if args.account is None:
                print ("account was not provided.")        
                return
            elif args.warehouse is None:
                print ("warehouse was not provided.")        
                return
            elif args.username is None:
                print ("username was not provided.")        
                return
            elif (args.authenticator =="password" and args.password is None) or (args.authenticator=='username_password_mfa' and args.password is None):
                args.password =getpass.getpass()        
                sections_builder()                   
            elif args.role is None:
                print ("role was not provided.")        
                return
            elif args.authenticator=='username_password_mfa' and args.token is None:
                print ("Token (-k) must be provided when using username_password_mfa authentication.")            
            else:
                sections_builder()
    elif args.type=='databricks':
        print ("Feature under development.")
    else:
        print ("Wrong argument.")

def create_snowflake_db_connection(authenticator):
    
    try:
        if authenticator=='password':
            conn = snowflake.connector.connect(
                user=args.username,
                account=args.account,
                password=args.password,
                warehouse=args.warehouse,
                role=args.role
            )
            cur = conn.cursor()
            cur.execute("USE WAREHOUSE "+args.warehouse) 
            print("Snowflake connection Opened. ")
            return conn
        elif authenticator=='externalbrowser':
            conn = snowflake.connector.connect(
                user=args.username,
                account=args.account,
                warehouse=args.warehouse,
                role=args.role,
                authenticator="externalbrowser"
            )
            cur = conn.cursor()
            cur.execute("USE WAREHOUSE "+args.warehouse) 
            print("Snowflake connection Opened. ")
            return conn
        elif authenticator=='username_password_mfa':
            conn = snowflake.connector.connect(
                user=args.username,
                password=args.password,
                account=args.account,
                warehouse=args.warehouse,
                role=args.role,
                passcode=args.token,
                authenticator="username_password_mfa"
            )
            cur = conn.cursor()
            cur.execute("USE WAREHOUSE "+args.warehouse) 
            print("Snowflake connection Opened. ")
            return conn
        else:
            return -1            
    except Exception as error:
        print("Error while opening connection to Snowflake:", error)
        return -1

def close_snowflake_db_connection(conn):
    try:
        conn.cursor().close()
    except Exception as error:
        print("Error while closing cursor for Snowflake:", error)
    try:  
        conn.close()
        print("Snowflake connection closed. ")
    except Exception as error:
        print("Error while closing connection for Snowflake:", error)
        return -1

def create_output_file(file_name, file_content):
    global report_formatted_time
    
    try:
        REPORTS_FOLDER = 'prismafy-reports'
        FILE_NAME = REPORTS_FOLDER+'/'+report_root_folder+'/'+file_name                
        print (datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + "  Creating "+file_name)
        os.makedirs(os.path.dirname(FILE_NAME), exist_ok=True)
        fh = open(FILE_NAME, 'w',encoding='utf-8')
        fh.write(file_content)
        fh.close()    
    except Exception as error:
        print(datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + "     ["+file_name+"]: An exception occurred while creating the html file:", error)

def move_icon():
    try:
        REPORTS_FOLDER = 'prismafy-reports'        
        shutil.copyfile('./prismafy.png', REPORTS_FOLDER+'/'+report_root_folder+'/prismafy.png')
        shutil.copyfile('./prismafy_font.png', REPORTS_FOLDER+'/'+report_root_folder+'/prismafy_font.png')
    except Exception as error:
        print ("Error while copying icon: "+error)

def generate_top_query_info(conn):

    try:    
        global snowflake_conn        
        conn =snowflake_conn
        
        sql_query=sql_header+"""
        WITH DATA AS (
        SELECT 
            QUERY_PARAMETERIZED_HASH                            AS QUERY_PARAMETERIZED_HASH,
            ROUND(SUM(TOTAL_ELAPSED_TIME)/1000,2)               AS QUERY_EXECUTION_TIME_SECONDS
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
        WHERE  TO_DATE(Q.START_TIME) > DATEADD(MONTH,-1,TO_TIMESTAMP("""+report_formatted_time+"""))
            AND TOTAL_ELAPSED_TIME > 0
            AND ERROR_CODE IS NULL
            AND QUERY_PARAMETERIZED_HASH IS NOT NULL
        GROUP BY 1
        )
        SELECT 
            QUERY_PARAMETERIZED_HASH,
            QUERY_EXECUTION_TIME_SECONDS
        FROM DATA
        ORDER BY QUERY_EXECUTION_TIME_SECONDS DESC 
        LIMIT 10
        """
        cur = conn.cursor()
        cur.execute(sql_query)
        
        if int(cur.rowcount)!=0:
            for (QUERY_PARAMETERIZED_HASH, QUERY_EXECUTION_TIME_SECONDS) in cur:
                line_history_bytes_details_by_query_parameterized_hash(conn,QUERY_PARAMETERIZED_HASH)
                line_history_calls_details_by_query_parameterized_hash(conn,QUERY_PARAMETERIZED_HASH)
                line_history_time_details_by_query_parameterized_hash(conn,QUERY_PARAMETERIZED_HASH)
                line_history_rows_details_by_query_parameterized_hash(conn,QUERY_PARAMETERIZED_HASH)
                table_last_executions_of_query(conn,QUERY_PARAMETERIZED_HASH)
                line_history_wh_changes_by_query(conn,QUERY_PARAMETERIZED_HASH)
                table_history_accessed_objects_by_query(conn,QUERY_PARAMETERIZED_HASH)

    except Exception as error:
        print("[generate_top_query_info]: An exception occurred:", error)

def table_month_top_query(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global months_history
        global report_sections
        conn =snowflake_conn         

        iterations = ["query_execution_time_seconds", "partitions_scanned","percentage_scanned_from_cache", "rows_produced","gb_spilled_to_local_storage","gb_spilled_to_remote_storage","gb_sent_over_the_network","compilation_time_seconds","queued_provisioning_time_seconds","queued_overload_time_seconds","query_load_percent"]
        
        for iteration in iterations:
            
            html_file=html_table_header

            sql_query=sql_header+"""
                WITH DATA AS (
                SELECT DATE_TRUNC('HOUR',START_TIME)                        AS START_TIME , 
                  QUERY_PARAMETERIZED_HASH                                  AS QUERY_PARAMETERIZED_HASH,
                  REPLACE(REPLACE(QUERY_TEXT,'\n',' '),'<td>')              AS QUERY_TEXT,
                  ROUND(TOTAL_ELAPSED_TIME/1000,2)                          AS QUERY_EXECUTION_TIME_SECONDS,
                  ROUND(PARTITIONS_SCANNED,2)                               AS PARTITIONS_SCANNED,
                  ROUND(PARTITIONS_TOTAL,2)                                 AS PARTITIONS_TOTAL,
                  ROUND(TO_NUMBER(PERCENTAGE_SCANNED_FROM_CACHE,10,2),2)    AS PERCENTAGE_SCANNED_FROM_CACHE,
                  ROUND(BYTES_READ_FROM_RESULT/1024/1024/1024,2)            AS GB_READ_FROM_RESULT,
                  ROUND(ROWS_PRODUCED,2)                                    AS ROWS_PRODUCED,
                  ROUND(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024,2)    AS GB_SPILLED_TO_LOCAL_STORAGE,
                  ROUND(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024,2)   AS GB_SPILLED_TO_REMOTE_STORAGE,
                  ROUND(BYTES_SENT_OVER_THE_NETWORK/1024/1024/1024  ,2)     AS GB_SENT_OVER_THE_NETWORK,
                  ROUND(COMPILATION_TIME/1000,2)                            AS COMPILATION_TIME_SECONDS,
                  ROUND(EXECUTION_TIME/1000 ,2)                             AS EXECUTION_TIME_SECONDS,
                  ROUND(QUEUED_PROVISIONING_TIME/1000 ,2)                   AS QUEUED_PROVISIONING_TIME_SECONDS,
                  ROUND(QUEUED_OVERLOAD_TIME/1000   ,2)                     AS QUEUED_OVERLOAD_TIME_SECONDS,
                  ROUND(TRANSACTION_BLOCKED_TIME/1000  ,2)                  AS TRANSACTION_BLOCKED_TIME_SECONDS,
                  ROUND(QUERY_LOAD_PERCENT,2)                               AS QUERY_LOAD_PERCENT
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
                WHERE  TO_DATE(Q.START_TIME) > DATEADD(MONTH,-1,TO_TIMESTAMP("""+report_formatted_time+"""))
                  AND TOTAL_ELAPSED_TIME > 0
                  AND ERROR_CODE IS NULL
                  AND """+ iteration +""" IS NOT NULL
                ORDER BY """+ iteration +""" DESC
                LIMIT 50
                )
                SELECT 
                    ROW_NUMBER() over (order by """+ iteration +"""  DESC)       AS TOP_N
                    ,START_TIME
                    ,QUERY_PARAMETERIZED_HASH
                    ,QUERY_TEXT
                    ,QUERY_EXECUTION_TIME_SECONDS
                    ,PARTITIONS_SCANNED
                    ,PARTITIONS_TOTAL
                    ,PERCENTAGE_SCANNED_FROM_CACHE
                    ,GB_READ_FROM_RESULT
                    ,ROWS_PRODUCED
                    ,GB_SPILLED_TO_LOCAL_STORAGE
                    ,GB_SPILLED_TO_REMOTE_STORAGE
                    ,GB_SENT_OVER_THE_NETWORK
                    ,COMPILATION_TIME_SECONDS
                    ,EXECUTION_TIME_SECONDS
                    ,QUEUED_PROVISIONING_TIME_SECONDS
                    ,QUEUED_OVERLOAD_TIME_SECONDS
                    ,TRANSACTION_BLOCKED_TIME_SECONDS
                    ,QUERY_LOAD_PERCENT
                FROM DATA
                ORDER BY TOP_N;
            """
            
            cur = conn.cursor()
            cur.execute(sql_query)
        
            html_file=html_file+"""
            <h3>Top query for """+ iteration.lower() +""" for last month</h3>
            <table class="tabla2">
            <tr>
            <th >TOP_N</th>
            <th >DATE</th>
            <th >QUERY_PARAMETERIZED_HASH</th>
            <th >QUERY_EXECUTION_TIME_SECONDS</th>
            <th >PARTITIONS_SCANNED</th>
            <th >PARTITIONS_TOTAL</th>
            <th >PERCENTAGE_SCANNED_FROM_CACHE</th>
            <th >GB_READ_FROM_RESULT</th>
            <th >ROWS_PRODUCED</th>
            <th >GB_SPILLED_TO_LOCAL_STORAGE</th>
            <th >GB_SPILLED_TO_REMOTE_STORAGE</th>
            <th >GB_SENT_OVER_THE_NETWORK</th>
            <th >COMPILATION_TIME_SECONDS</th>
            <th >EXECUTION_TIME_SECONDS</th>
            <th >QUEUED_PROVISIONING_TIME_SECONDS</th>
            <th >QUEUED_OVERLOAD_TIME_SECONDS</th>
            <th >TRANSACTION_BLOCKED_TIME_SECONDS</th>
            <th >QUERY_LOAD_PERCENT</th>
            <th >QUERY_TEXT</th>
            """
        
            if int(cur.rowcount)!=0:
                for (ROW_TOP_N, ROW_START_TIME, ROW_QUERY_PARAMETERIZED_HASH, ROW_QUERY_TEXT, ROW_QUERY_EXECUTION_TIME_SECONDS, ROW_PARTITIONS_SCANNED, ROW_PARTITIONS_TOTAL, ROW_PERCENTAGE_SCANNED_FROM_CACHE, ROW_GB_READ_FROM_RESULT, ROW_ROWS_PRODUCED, ROW_GB_SPILLED_TO_LOCAL_STORAGE, ROW_GB_SPILLED_TO_REMOTE_STORAGE, ROW_GB_SENT_OVER_THE_NETWORK, ROW_COMPILATION_TIME_SECONDS, ROW_EXECUTION_TIME_SECONS, ROW_QUEUED_PROVISIONING_TIME_SECONDS, ROW_QUEUED_OVERLOAD_TIME_SECONDS, ROW_TRANSACTION_BLOCKED_TIME_SECONDS, ROW_QUERY_LOAD_PERCENT) in cur:
                    html_file=html_file+""" <tr> 
                    <td>"""+str(ROW_TOP_N)+"""</td> 
                    <td>"""+str(ROW_START_TIME)+"""</td> 
                     <td>"""+str(ROW_QUERY_PARAMETERIZED_HASH)+"""</td> 
                     <td>"""+str(ROW_QUERY_EXECUTION_TIME_SECONDS)+"""</td> 
                     <td>"""+str(ROW_PARTITIONS_SCANNED)+"""</td> 
                     <td>"""+str(ROW_PARTITIONS_TOTAL)+"""</td> 
                     <td>"""+str(ROW_PERCENTAGE_SCANNED_FROM_CACHE)+"""</td> 
                     <td>"""+str(ROW_GB_READ_FROM_RESULT)+"""</td> 
                     <td>"""+str(ROW_ROWS_PRODUCED)+"""</td> 
                     <td>"""+str(ROW_GB_SPILLED_TO_LOCAL_STORAGE)+"""</td> 
                     <td>"""+str(ROW_GB_SPILLED_TO_REMOTE_STORAGE)+"""</td> 
                     <td>"""+str(ROW_GB_SENT_OVER_THE_NETWORK)+"""</td> 
                     <td>"""+str(ROW_COMPILATION_TIME_SECONDS)+"""</td>
                     <td>"""+str(ROW_EXECUTION_TIME_SECONS)+"""</td> 
                     <td>"""+str(ROW_QUEUED_PROVISIONING_TIME_SECONDS)+"""</td> 
                     <td>"""+str(ROW_QUEUED_OVERLOAD_TIME_SECONDS)+"""</td> 
                     <td>"""+str(ROW_TRANSACTION_BLOCKED_TIME_SECONDS)+"""</td> 
                     <td>"""+str(ROW_QUERY_LOAD_PERCENT)+"""</td> 
                     <td class="cell_grow">"""+str(ROW_QUERY_TEXT.replace("\\n"," "))+"""</td> 
                    </tr> """
                
                html_file=html_file+html_table_tail
        
                create_output_file('last_month_top_query_for_'+ iteration.lower() +'.html',html_file)
                report_sections["D - Performance"].update({'last_month_top_query_for_'+ iteration.lower() +'.html':'table'})
    
    except Exception as error:
        print("[table_month_top_query]: An exception occurred:", error)

def table_week_top_query(conn):
    try:
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn
        
        iterations = ["query_execution_time_seconds", "partitions_scanned","percentage_scanned_from_cache", "rows_produced","gb_spilled_to_local_storage","gb_spilled_to_remote_storage","gb_sent_over_the_network","compilation_time_seconds","queued_provisioning_time_seconds","queued_overload_time_seconds","query_load_percent"]
        
        for iteration in iterations:
            
            html_file=html_table_header
            
            sql_query=sql_header+"""
                WITH DATA AS (
                SELECT DATE_TRUNC('HOUR',START_TIME)                        AS START_TIME , 
                  QUERY_PARAMETERIZED_HASH                                  AS QUERY_PARAMETERIZED_HASH,
                  REPLACE(REPLACE(query_text,'\n',' '),'<td>')              AS QUERY_TEXT,
                  ROUND(TOTAL_ELAPSED_TIME/1000,2)                          AS QUERY_EXECUTION_TIME_SECONDS,
                  ROUND(PARTITIONS_SCANNED,2)                               AS PARTITIONS_SCANNED,
                  ROUND(PARTITIONS_TOTAL,2)                                 AS PARTITIONS_TOTAL,
                  ROUND(TO_NUMBER(PERCENTAGE_SCANNED_FROM_CACHE,10,2),2)    AS PERCENTAGE_SCANNED_FROM_CACHE,
                  ROUND(BYTES_READ_FROM_RESULT/1024/1024/1024,2)            AS GB_READ_FROM_RESULT,
                  ROUND(ROWS_PRODUCED,2)                                    AS ROWS_PRODUCED,
                  ROUND(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024,2)    AS GB_SPILLED_TO_LOCAL_STORAGE,
                  ROUND(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024,2)   AS GB_SPILLED_TO_REMOTE_STORAGE,
                  ROUND(BYTES_SENT_OVER_THE_NETWORK/1024/1024/1024  ,2)     AS GB_SENT_OVER_THE_NETWORK,
                  ROUND(COMPILATION_TIME/1000,2)                            AS COMPILATION_TIME_SECONDS,
                  ROUND(EXECUTION_TIME/1000 ,2)                             AS EXECUTION_TIME_SECONDS,
                  ROUND(QUEUED_PROVISIONING_TIME/1000 ,2)                   AS QUEUED_PROVISIONING_TIME_SECONDS,
                  ROUND(QUEUED_OVERLOAD_TIME/1000   ,2)                     AS QUEUED_OVERLOAD_TIME_SECONDS,
                  ROUND(TRANSACTION_BLOCKED_TIME/1000  ,2)                  AS TRANSACTION_BLOCKED_TIME_SECONDS,
                  ROUND(QUERY_LOAD_PERCENT,2)                               AS QUERY_LOAD_PERCENT
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
                WHERE  TO_DATE(Q.START_TIME) > DATEADD(DAY,-7,TO_TIMESTAMP("""+report_formatted_time+"""))
                  AND TOTAL_ELAPSED_TIME > 0 
                  AND ERROR_CODE IS NULL
                  AND """+ iteration +""" IS NOT NULL
                ORDER BY """+ iteration +""" DESC
                LIMIT 50
                )
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY """+ iteration +"""  DESC)       AS TOP_N
                    ,START_TIME
                    ,QUERY_PARAMETERIZED_HASH
                    ,QUERY_TEXT
                    ,NVL(QUERY_EXECUTION_TIME_SECONDS,0)
                    ,NVL(PARTITIONS_SCANNED,0)
                    ,NVL(PARTITIONS_TOTAL,0)
                    ,NVL(PERCENTAGE_SCANNED_FROM_CACHE,0)
                    ,NVL(GB_READ_FROM_RESULT,0)
                    ,NVL(ROWS_PRODUCED,0)
                    ,NVL(GB_SPILLED_TO_LOCAL_STORAGE,0)
                    ,NVL(GB_SPILLED_TO_REMOTE_STORAGE,0)
                    ,NVL(GB_SENT_OVER_THE_NETWORK,0)
                    ,NVL(COMPILATION_TIME_SECONDS,0)
                    ,NVL(EXECUTION_TIME_SECONDS,0)
                    ,NVL(QUEUED_PROVISIONING_TIME_SECONDS,0)
                    ,NVL(QUEUED_OVERLOAD_TIME_SECONDS,0)
                    ,NVL(TRANSACTION_BLOCKED_TIME_SECONDS,0)
                    ,NVL(QUERY_LOAD_PERCENT,0)
                FROM DATA
                ORDER BY TOP_N;
            """
            
            cur = conn.cursor()
            cur.execute(sql_query)
        
            html_file=html_file+"""
            <h3>Top query for """+ iteration.lower() +""" for last week</h3>
            <table class="tabla2">
            <tr>
            <th >TOP_N</th>
            <th >DATE</th>
            <th >QUERY_PARAMETERIZED_HASH</th>
            <th >QUERY_EXECUTION_TIME_SECONDS</th>
            <th >PARTITIONS_SCANNED</th>
            <th >PARTITIONS_TOTAL</th>
            <th >PERCENTAGE_SCANNED_FROM_CACHE</th>
            <th >GB_READ_FROM_RESULT</th>
            <th >ROWS_PRODUCED</th>
            <th >GB_SPILLED_TO_LOCAL_STORAGE</th>
            <th >GB_SPILLED_TO_REMOTE_STORAGE</th>
            <th >GB_SENT_OVER_THE_NETWORK</th>
            <th >COMPILATION_TIME_SECONDS</th>
            <th >EXECUTION_TIME_SECONDS</th>
            <th >QUEUED_PROVISIONING_TIME_SECONDS</th>
            <th >QUEUED_OVERLOAD_TIME_SECONDS</th>
            <th >TRANSACTION_BLOCKED_TIME_SECONDS</th>
            <th >QUERY_LOAD_PERCENT</th>
            <th >QUERY_TEXT</th>
            """
        
            if int(cur.rowcount)!=0:
                for (ROW_TOP_N, ROW_START_TIME, ROW_QUERY_PARAMETERIZED_HASH, ROW_QUERY_TEXT, ROW_QUERY_EXECUTION_TIME_SECONDS, ROW_PARTITIONS_SCANNED, ROW_PARTITIONS_TOTAL, ROW_PERCENTAGE_SCANNED_FROM_CACHE, ROW_GB_READ_FROM_RESULT, ROW_ROWS_PRODUCED, ROW_GB_SPILLED_TO_LOCAL_STORAGE, ROW_GB_SPILLED_TO_REMOTE_STORAGE, ROW_GB_SENT_OVER_THE_NETWORK, ROW_COMPILATION_TIME_SECONDS, ROW_EXECUTION_TIME_SECONS, ROW_QUEUED_PROVISIONING_TIME_SECONDS, ROW_QUEUED_OVERLOAD_TIME_SECONDS, ROW_TRANSACTION_BLOCKED_TIME_SECONDS, ROW_QUERY_LOAD_PERCENT) in cur:
                    html_file=html_file+""" <tr> 
                    <td>"""+str(ROW_TOP_N)+"""</td> 
                    <td>"""+str(ROW_START_TIME)+"""</td> 
                     <td>"""+str(ROW_QUERY_PARAMETERIZED_HASH)+"""</td> 
                     <td>"""+str(ROW_QUERY_EXECUTION_TIME_SECONDS)+"""</td> 
                     <td>"""+str(ROW_PARTITIONS_SCANNED)+"""</td> 
                     <td>"""+str(ROW_PARTITIONS_TOTAL)+"""</td> 
                     <td>"""+str(ROW_PERCENTAGE_SCANNED_FROM_CACHE)+"""</td> 
                     <td>"""+str(ROW_GB_READ_FROM_RESULT)+"""</td> 
                     <td>"""+str(ROW_ROWS_PRODUCED)+"""</td> 
                     <td>"""+str(ROW_GB_SPILLED_TO_LOCAL_STORAGE)+"""</td> 
                     <td>"""+str(ROW_GB_SPILLED_TO_REMOTE_STORAGE)+"""</td> 
                     <td>"""+str(ROW_GB_SENT_OVER_THE_NETWORK)+"""</td> 
                     <td>"""+str(ROW_COMPILATION_TIME_SECONDS)+"""</td>
                     <td>"""+str(ROW_EXECUTION_TIME_SECONS)+"""</td> 
                     <td>"""+str(ROW_QUEUED_PROVISIONING_TIME_SECONDS)+"""</td> 
                     <td>"""+str(ROW_QUEUED_OVERLOAD_TIME_SECONDS)+"""</td> 
                     <td>"""+str(ROW_TRANSACTION_BLOCKED_TIME_SECONDS)+"""</td> 
                     <td>"""+str(ROW_QUERY_LOAD_PERCENT)+"""</td> 
                     <td class="cell_grow">"""+str(ROW_QUERY_TEXT)+"""</td> 
                    </tr> """
                
                html_file=html_file+html_table_tail

                create_output_file('last_week_top_query_for_'+ iteration.lower() +'.html',html_file)
                report_sections["D - Performance"].update({'last_week_top_query_for_'+ iteration.lower() +'.html':'table'})
        
    except Exception as error:
        print("[table_week_top_query]: An exception occurred:", error)
            
def line_history_bytes_details_by_query_parameterized_hash(conn,sql_query_id):
    try:    
        global snowflake_conn
        global html_body1
        global html_line_hour_tail
        global report_sections
        conn =snowflake_conn
                            
        html_file=html_header+"""
        [
        'DATE',
        'GB_READ_FROM_RESULT',
        'GB_SPILLED_TO_LOCAL_STORAGE',
        'GB_SPILLED_TO_REMOTE_STORAGE',
        'GB_SENT_OVER_THE_NETWORK',
        'GB_WRITTEN',
        'GB_WRITTEN_TO_RESULT',
        'GB_SCANNED',
        'GB_DELETED',
        'OUTBOUND_DATA_TRANSFER_GB',
        'INBOUND_DATA_TRANSFER_GB',
        'EXTERNAL_FUNCTION_TOTAL_SENT_GB',
        'EXTERNAL_FUNCTION_TOTAL_RECEIVED_GB',
        'QUERY_ACCELERATION_GB_SCANNED'
        ],
        """
        
        sql_query_details=sql_header+"""
        WITH DATA AS (
        SELECT DATE_TRUNC('HOUR',START_TIME::TIMESTAMP_NTZ)                          AS START_TIME , 
        ROUND(MAX(NVL(BYTES_READ_FROM_RESULT,0)/1024/1024/1024),2)                   AS GB_READ_FROM_RESULT,
        ROUND(MAX(NVL(BYTES_SPILLED_TO_LOCAL_STORAGE,0)/1024/1024/1024),2)           AS GB_SPILLED_TO_LOCAL_STORAGE,
        ROUND(MAX(NVL(BYTES_SPILLED_TO_REMOTE_STORAGE,0)/1024/1024/1024),2)          AS GB_SPILLED_TO_REMOTE_STORAGE,
        ROUND(MAX(NVL(BYTES_SENT_OVER_THE_NETWORK,0)/1024/1024/1024 ),2)             AS GB_SENT_OVER_THE_NETWORK,
        ROUND(MAX(NVL(BYTES_WRITTEN,0)/1024/1024/1024),2)                            AS GB_WRITTEN,
        ROUND(MAX(NVL(BYTES_WRITTEN_TO_RESULT,0)/1024/1024/1024),2)                  AS GB_WRITTEN_TO_RESULT,
        ROUND(MAX(NVL(BYTES_SCANNED,0)/1024/1024/1024),2)                            AS GB_SCANNED   ,
        ROUND(MAX(NVL(BYTES_DELETED,0)/1024/1024/1024),2)                            AS GB_DELETED,
        ROUND(MAX(NVL(OUTBOUND_DATA_TRANSFER_BYTES,0)/1024/1024/1024),2)             AS OUTBOUND_DATA_TRANSFER_GB,
        ROUND(MAX(NVL(INBOUND_DATA_TRANSFER_BYTES,0)/1024/1024/1024),2)              AS INBOUND_DATA_TRANSFER_GB,
        ROUND(MAX(NVL(EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,0)/1024/1024/1024),2)       AS EXTERNAL_FUNCTION_TOTAL_SENT_GB,
        ROUND(MAX(NVL(EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,0)/1024/1024/1024),2)   AS EXTERNAL_FUNCTION_TOTAL_RECEIVED_GB,
        ROUND(MAX(NVL(QUERY_ACCELERATION_BYTES_SCANNED,0)/1024/1024/1024),2)         AS QUERY_ACCELERATION_GB_SCANNED
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
        WHERE  TO_DATE(Q.START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
        AND TOTAL_ELAPSED_TIME > 0 
        AND QUERY_PARAMETERIZED_HASH='"""+str(sql_query_id)+"""'
        GROUP BY 1
        ORDER BY START_TIME  
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query_details)
        
        if int(cur_details.rowcount)!=0:
            for (row_data) in cur_details:
                html_file=html_file+str(row_data[0])+""","""
            
            html_file=html_file+html_body1+"""
            row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13]
            """+html_body2+"""
            trendlines: {6:{type: 'linear', color: '#16F529', labelInLegend: 'Trend for gb_scanned', visibleInLegend: true, opacity: 0.6, pointsVisible: false, lineWidth:2}},
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Bytes Details for query """+str(sql_query_id)+""" in the last month`,
            """+html_line_hour_tail
        
            create_output_file('history_bytes_details_for_'+  sql_query_id.lower()  +'.html',html_file)
            report_sections["D - Performance"].update({'history_bytes_details_for_'+  sql_query_id.lower()  +'.html':'line'})
            
    except Exception as error:
        print("[line_history_bytes_details_by_query_parameterized_hash]: An exception occurred:", error) 

def line_history_calls_details_by_query_parameterized_hash(conn,sql_query_id):

    try:        
        global snowflake_conn
        conn =snowflake_conn
        global report_sections
        
        html_file=html_header+"""
        [
        'START_TIME',
        'CALLS'
        ],
        """

        sql_query_details=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('HOUR',INTERVAL_START_TIME::TIMESTAMP_NTZ)    AS START_TIME , 
                SUM(NVL(CALLS,0))                                           AS CALLS
            FROM SNOWFLAKE.ACCOUNT_USAGE.AGGREGATE_QUERY_HISTORY  Q
            WHERE  TO_DATE(Q.INTERVAL_START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND QUERY_PARAMETERIZED_HASH='"""+str(sql_query_id)+"""'
            GROUP BY 1
            ORDER BY START_TIME  
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query_details)
        
        if int(cur_details.rowcount)!=0:
            for (row_data) in cur_details:
                html_file=html_file+str(row_data[0])+""","""
            
            html_file=html_file+html_body1+"""
            row[1]
            """+html_body2+"""
            trendlines: {0:{type: 'linear', color: '#16F529', labelInLegend: 'Trend for calls', visibleInLegend: true, opacity: 0.6, pointsVisible: false, lineWidth:2}},
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Calls Detail for query """+str(sql_query_id)+"""`,"""+html_line_hour_tail

            create_output_file('calls_details_for_'+  sql_query_id.lower()  +'.html',html_file)
            report_sections["D - Performance"].update({'calls_details_for_'+  sql_query_id.lower()  +'.html':'line'})
        
    except Exception as error:
        print("[line_calls_details_by_query_parameterized_hash]: An exception occurred:", error)

def line_history_time_details_by_query_parameterized_hash(conn,sql_query_id):

    try:
        global snowflake_conn
        conn =snowflake_conn
        global report_sections
                        
        html_file=html_header+"""
        [
        'DATE',
        'QUERY_EXECUTION_TIME_SECONDS',
        'COMPILATION_TIME_SECONDS',
        'EXECUTION_TIME_SECONDS',
        'QUEUED_PROVISIONING_TIME_SECONDS',
        'QUEUED_OVERLOAD_TIME_SECONDS',
        'TRANSACTION_BLOCKED_TIME_SECONDS',
        'QUEUED_REPAIR_TIME_SECONDS',
        'LIST_EXTERNAL_FILES_TIME_SECONDS',
        'CHILD_QUERIES_WAIT_TIME_SECONDS',
        'QUERY_RETRY_TIME_SECONDS',
        'FAULT_HANDLING_TIME_SECONDS'
        ],
        """

        sql_query_details=sql_header+"""
        WITH DATA AS (
        SELECT DATE_TRUNC('HOUR',START_TIME::TIMESTAMP_NTZ)            AS START_TIME , 
            ROUND(MAX(NVL(total_elapsed_time,0))/1000,2)                 AS QUERY_EXECUTION_TIME_SECONDS,
            ROUND(MAX(NVL(COMPILATION_TIME,0))/1000,2)                   AS COMPILATION_TIME_SECONDS,
            ROUND(MAX(NVL(EXECUTION_TIME,0))/1000,2)                     AS EXECUTION_TIME_SECONDS,
            ROUND(MAX(NVL(QUEUED_PROVISIONING_TIME,0) )/1000,2)          AS QUEUED_PROVISIONING_TIME_SECONDS,
            ROUND(MAX(NVL(QUEUED_OVERLOAD_TIME,0))/1000,2)               AS QUEUED_OVERLOAD_TIME_SECONDS,
            ROUND(MAX(NVL(TRANSACTION_BLOCKED_TIME,0))/1000,2)           AS TRANSACTION_BLOCKED_TIME_SECONDS,
            ROUND(MAX(NVL(QUEUED_REPAIR_TIME,0))/1000,2)                 AS QUEUED_REPAIR_TIME_SECONDS   ,
            ROUND(MAX(NVL(LIST_EXTERNAL_FILES_TIME,0))/1000,2)           AS LIST_EXTERNAL_FILES_TIME_SECONDS,
            ROUND(MAX(NVL(CHILD_QUERIES_WAIT_TIME,0))/1000,2)            AS CHILD_QUERIES_WAIT_TIME_SECONDS,
            ROUND(MAX(NVL(QUERY_RETRY_TIME,0))/1000,2)                   AS QUERY_RETRY_TIME_SECONDS,
            ROUND(MAX(NVL(FAULT_HANDLING_TIME,0))/1000,2)                AS FAULT_HANDLING_TIME_SECONDS,
        FROM snowflake.account_usage.query_history Q
        WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND total_elapsed_time > 0 
            and query_parameterized_hash='"""+str(sql_query_id)+"""'
        GROUP BY 1
        ORDER BY START_TIME  
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query_details)
        
        if int(cur_details.rowcount)!=0:
            for (row_data) in cur_details:
                html_file=html_file+str(row_data[0])+""","""
            
            html_file=html_file+  html_body1 +"""
            row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11]
            """+ html_body2 +"""
            trendlines: {0:{type: 'linear', color: '#16F529', labelInLegend: 'Trend for query_execution_time_seconds', visibleInLegend: true, opacity: 0.6, pointsVisible: false, lineWidth:2}},
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Time Details for query """+str(sql_query_id)+"""`,"""+ html_line_hour_tail  

            create_output_file('time_details_for_'+  sql_query_id.lower()  +'.html',html_file)
            report_sections["D - Performance"].update({'time_details_for_'+ sql_query_id.lower()  +'.html':'line'})
        
    except Exception as error:
        print("[line_time_details_by_query_parameterized_hash]: An exception occurred:", error)

def line_history_rows_details_by_query_parameterized_hash(conn,sql_query_id):
    try:
        
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
                            
        html_file=html_header+"""
        [
        'DATE',
        'ROWS_PRODUCED',
        'ROWS_INSERTED',
        'ROWS_UPDATED',
        'ROWS_DELETED',
        'ROWS_UNLOADED',
        'EXTERNAL_FUNCTION_TOTAL_SENT_ROWS',
        'EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS',
        'ROWS_WRITTEN_TO_RESULT'
        ],
        """
        sql_query_details=sql_header+"""
        WITH DATA AS (
        SELECT DATE_TRUNC('HOUR',START_TIME::TIMESTAMP_NTZ)            AS START_TIME , 
            ROUND(MAX(NVL(ROWS_PRODUCED,0)),2)                           AS ROWS_PRODUCED,
            ROUND(MAX(NVL(ROWS_INSERTED,0)),2)                           AS ROWS_INSERTED,
            ROUND(MAX(NVL(ROWS_UPDATED,0)),2)                            AS ROWS_UPDATED,
            ROUND(MAX(NVL(ROWS_DELETED,0) ),2)                           AS ROWS_DELETED,
            ROUND(MAX(NVL(ROWS_UNLOADED,0)),2)                           AS ROWS_UNLOADED,
            ROUND(MAX(NVL(EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,0)),2)       AS EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,
            ROUND(MAX(NVL(EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,0)),2)   AS EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS   ,
            ROUND(MAX(NVL(ROWS_WRITTEN_TO_RESULT,0)),2)                  AS ROWS_WRITTEN_TO_RESULT,
        FROM snowflake.account_usage.query_history Q
        WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND total_elapsed_time > 0 --only get queries that actually used compute
            and query_parameterized_hash='"""+str(sql_query_id)+"""'
        GROUP BY 1
        ORDER BY START_TIME  
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query_details)
        
        if int(cur_details.rowcount)!=0:
            for (row_data) in cur_details:
                html_file=html_file+str(row_data[0])+""","""            
            
            html_file=html_file+html_body1+"""
            row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]
            """+html_body2+"""
            trendlines: {0:{type: 'linear', color: '#16F529', labelInLegend: 'Trend for rows_produced', visibleInLegend: true, opacity: 0.6, pointsVisible: false, lineWidth:2}},
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Rows Details for query """+str(sql_query_id)+"""`,"""+html_line_hour_tail

            create_output_file('rows_details_for_'+  sql_query_id.lower()  +'.html',html_file)
            report_sections["D - Performance"].update({'rows_details_for_'+  sql_query_id.lower()  +'.html':'line'})
        
    except Exception as error:
        print("[line_rows_details_by_query_parameterized_hash]: An exception occurred:", error)

def line_history_account_consumption_credits_by_warehouse(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn        
        html_file=html_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT 
                DATE_TRUNC('DAY',START_TIME::TIMESTAMP_NTZ)  AS DATE, 
                WAREHOUSE_NAME                               AS WAREHOUSE_NAME,
                ROUND(SUM(CREDITS_USED),2)                   AS CREDITS_USED,
            FROM  SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
            WHERE
                TO_DATE(START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            GROUP BY 1,2
            ORDER BY 1 
        )
        , DATA_PIVOT AS (
            SELECT *
            FROM DATA
                PIVOT(  MAX (CREDITS_USED)  FOR WAREHOUSE_NAME IN (any order by WAREHOUSE_NAME) DEFAULT ON NULL (0) )    
            ORDER BY DATE 
        )
        SELECT *  FROM DATA_PIVOT;
        """        
        cur = conn.cursor()
        cur.execute(sql_query)
        column_count=len(cur.description)
        headers =[i[0] for i in cur.description]
        html_file=html_file+str(headers).replace('"','')+", \n"
    
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT 
                DATE_TRUNC('DAY',START_TIME::TIMESTAMP_NTZ)  AS DATE, 
                WAREHOUSE_NAME                               AS WAREHOUSE_NAME,
                ROUND(SUM(CREDITS_USED),2)                   AS CREDITS_USED,
            FROM  SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
            WHERE
                TO_DATE(start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            GROUP BY 1,2
            ORDER BY 1 
        )
        , DATA_PIVOT AS (
        SELECT *
        FROM DATA
            PIVOT(  MAX (CREDITS_USED)  FOR WAREHOUSE_NAME IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) )    
        ORDER BY DATE 
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA_PIVOT;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query)
    
        if int(cur_details.rowcount)!=0:
            counter=0
            for (row_data) in cur_details:
                counter=counter+1
                if counter==cur_details.rowcount:
                    html_file=html_file+str(row_data[0])
                else:
                    html_file=html_file+str(row_data[0])+""","""
    
            html_file=html_file+html_body1
    		
            for i in range(1, column_count):
                if i==column_count-1:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""
    		
            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Warehouse Credits Consumption`,"""+html_line_hour_tail
            
            create_output_file('credits_consumption_by_warehouse.html',html_file)
            report_sections["C - Credits"].update({'credits_consumption_by_warehouse.html':'line'})
            
    except Exception as error:
        print("[line_history_account_consumption_credits_by_warehouse]: An exception occurred:", error)

def line_history_account_consumption_credits(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        
        html_file=html_header+"""
            ['DATE','CREDITS_USED'],
        """
        
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT 
                DATE_TRUNC('DAY',START_TIME::TIMESTAMP_NTZ)  AS DATE, 
                ROUND(SUM(CREDITS_USED),2)                   AS CREDITS_USED,
            FROM  snowflake.account_usage.WAREHOUSE_METERING_HISTORY 
            WHERE
                TO_DATE(START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            GROUP BY 1
            ORDER BY 1 
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query)
    
        if int(cur_details.rowcount)!=0:
            counter=0
            for (row_data) in cur_details:
                counter=counter+1
                if counter==cur_details.rowcount:
                    html_file=html_file+str(row_data[0])
                else:
                    html_file=html_file+str(row_data[0])+""","""
    
            html_file=html_file+html_body1+"""
            row[1]
            """+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Credits Consumption for all warehouses`,"""+html_line_hour_tail
                
            create_output_file('warehouse_consumption.html',html_file)
            report_sections["C - Credits"].update({'warehouse_consumption.html':'line'})
            
    except Exception as error:
        print("[line_history_account_consumption_credits]: An exception occurred:", error)

def bar_month_consumption_credits_by_warehouse(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        
        html_file=html_header
        
        sql_query=sql_header+"""
            WITH DATA AS (
            SELECT 
                DATE_TRUNC('MONTH',START_TIME::TIMESTAMP_NTZ)   AS MONTH, 
                WAREHOUSE_NAME                                  AS WAREHOUSE_NAME,
                ROUND(SUM(CREDITS_USED),2)                      AS CREDITS_USED,
            FROM  SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
            WHERE
                TO_DATE(start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            GROUP BY 1,2
            ORDER BY 1 
        )
        , DATA_PIVOT AS (
        SELECT *
        FROM DATA
            PIVOT(  MAX (CREDITS_USED)  FOR WAREHOUSE_NAME IN (any order by WAREHOUSE_NAME) DEFAULT ON NULL (0) )    
        ORDER BY MONTH 
        )
        SELECT *  FROM DATA_PIVOT;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
        column_count=len(cur.description)
        headers =[i[0] for i in cur.description]
        html_file=html_file+str(headers).replace('"','')+", \n"
    
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT 
                DATE_TRUNC('MONTH',START_TIME::TIMESTAMP_NTZ)   AS MONTH, 
                WAREHOUSE_NAME                                  AS WAREHOUSE_NAME,
                ROUND(SUM(CREDITS_USED),2)                      AS CREDITS_USED,
            FROM  SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
            WHERE
                TO_DATE(START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            GROUP BY 1,2
            ORDER BY 1 
        )
        , DATA_PIVOT AS (
            SELECT *
            FROM DATA
                PIVOT(  MAX (CREDITS_USED)  FOR WAREHOUSE_NAME IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'MONTH',","")+""") DEFAULT ON NULL (0) )    
            ORDER BY MONTH 
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA_PIVOT;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query)
    
        if int(cur_details.rowcount)!=0:
            counter=0
            for (row_data) in cur_details:
                counter=counter+1
                if counter==cur_details.rowcount:
                    html_file=html_file+str(row_data[0])
                else:
                    html_file=html_file+str(row_data[0])+""","""
    
            html_file=html_file+html_body1
                
            for i in range(1, column_count):
                if i==column_count-1:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""
                
            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Warehouse Credits Consumption per month`,
            """+html_bar_month_tail
        
            create_output_file('per_month_credits_consumption_by_warehouse.html',html_file)
            report_sections["C - Credits"].update({'per_month_credits_consumption_by_warehouse.html':'bar'})
            
    except Exception as error:
        print("[bar_month_consumption_credits_by_warehouse]: An exception occurred:", error)

def bar_week_consumption_credits_by_warehouse(conn):
    try:  
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        
        html_file=html_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT 
                DATE_TRUNC('DAY',START_TIME::TIMESTAMP_NTZ)  AS DATE, 
                WAREHOUSE_NAME                               AS WAREHOUSE_NAME,
                ROUND(SUM(CREDITS_USED),2)                   AS CREDITS_USED,
            FROM  SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
            WHERE
                TO_DATE(START_TIME) > DATEADD(DAY,-7,TO_TIMESTAMP("""+report_formatted_time+"""))
            GROUP BY 1,2
            ORDER BY 1 
        )
        , DATA_PIVOT AS (
        SELECT *
        FROM DATA
            PIVOT(  MAX (CREDITS_USED)  FOR WAREHOUSE_NAME IN (any order by WAREHOUSE_NAME) DEFAULT ON NULL (0) )    
        ORDER BY DATE 
        )
        SELECT *  FROM DATA_PIVOT;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
        column_count=len(cur.description)
        headers =[i[0] for i in cur.description]
        html_file=html_file+str(headers).replace('"','')+", \n"
    
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT 
                DATE_TRUNC('DAY',START_TIME::TIMESTAMP_NTZ)     AS DATE, 
                WAREHOUSE_NAME                                  AS WAREHOUSE_NAME,
                ROUND(SUM(CREDITS_USED),2)                      AS CREDITS_USED,
            FROM  snowflake.account_usage.WAREHOUSE_METERING_HISTORY 
            WHERE
                TO_DATE(START_TIME) > DATEADD(DAY,-7,TO_TIMESTAMP("""+report_formatted_time+"""))
            GROUP BY 1,2
            ORDER BY 1 
        )
        , DATA_PIVOT AS (
        SELECT *
        FROM DATA
            PIVOT(  MAX (CREDITS_USED)  FOR WAREHOUSE_NAME IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) )    
        ORDER BY DATE 
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA_PIVOT;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query)
    
        if int(cur_details.rowcount)!=0:
            counter=0
            for (row_data) in cur_details:
                counter=counter+1
                if counter==cur_details.rowcount:
                    html_file=html_file+str(row_data[0])
                else:
                    html_file=html_file+str(row_data[0])+""","""
    
            html_file=html_file+html_body1
            
            for i in range(1, column_count):
                if i==column_count-1:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""
            
            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Warehouse credits consumption for last week`,
            """+html_bar_day_tail
        
            create_output_file('last_week_credits_consumption_by_warehouse.html',html_file)
            report_sections["C - Credits"].update({'last_week_credits_consumption_by_warehouse.html':'bar'})
            
    except Exception as error:
        print("[bar_week_consumption_credits_by_warehouse]: An exception occurred:", error)

def generate_warehouse_info(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        
        sql_query=sql_header+"""
        SELECT DISTINCT 
            WAREHOUSE_NAME
        FROM  SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
        WHERE
            TO_DATE(Start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND WAREHOUSE_NAME IS NOT NULL;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)

        if int(cur.rowcount)!=0:
            for (warehouse_name) in cur:
                line_history_load_details_by_warehouse(conn, warehouse_name[0])
                bar_month_load_details_by_warehouse(snowflake_conn,warehouse_name[0] )
                bar_week_load_details_by_warehouse(snowflake_conn,warehouse_name[0] )
                table_history_warehouse_events(snowflake_conn,warehouse_name[0] )
                line_history_warehouse_enable_vs_querycount(snowflake_conn,warehouse_name[0] )
                line_history_size_changes_by_warehouse(snowflake_conn,warehouse_name[0] )
            
    except Exception as error:
        print("[generate_warehouse_info]: An exception occurred:", error)
      
def line_history_load_details_by_warehouse(conn, warehouse_name):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
             
        sql_query_details=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('HOUR',start_time::TIMESTAMP_NTZ) AS START_TIME 
                ,ROUND(AVG(NVL(AVG_RUNNING,0)),2)                 AS RUNNING_LOAD
                ,ROUND(MAX(NVL(AVG_QUEUED_LOAD,0)),2)             AS QUEUED_LOAD
                ,ROUND(MAX(NVL(AVG_QUEUED_PROVISIONING,0)),2)     AS QUEUED_PROVISIONING_LOAD
                ,ROUND(MAX(NVL(AVG_BLOCKED,0)),2)                 AS BLOCKED_LOAD
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
            WHERE TO_DATE(START_TIME) >= DATEADD(month,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))    
            AND WAREHOUSE_NAME='"""+str(warehouse_name)+"""'
            GROUP BY 1
            ORDER BY 1
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA  FROM DATA;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query_details)
        html_file=html_header
        if int(cur_details.rowcount)!=0:
            html_file=html_file+"""
            ['DATE','RUNNING_LOAD','QUEUED_LOAD','QUEUED_PROVISIONING_LOAD','BLOCKED_LOAD'] ,
            """
            counter=0
            for (row_data) in cur_details:
                counter=counter+1
                if counter==cur_details.rowcount:
                    html_file=html_file+str(row_data[0])
                else:
                    html_file=html_file+str(row_data[0])+""","""

            html_file=html_file+html_body1+"""
            row[1],row[2],row[3],row[4]"""+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Load Details for warehouse """+warehouse_name+"""`,"""+html_line_hour_tail

            create_output_file('history_load_details_for_warehouse_'+warehouse_name.lower()+'.html',html_file)
            report_sections["A - Computing"].update({'history_load_details_for_warehouse_'+warehouse_name.lower()+'.html':'line'})
            
    except Exception as error:
        print("[line_history_load_details_by_warehouse]: An exception occurred:", error)
        
def bar_month_load_details_by_warehouse(conn,warehouse_name):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        html_file=html_header                
        sql_query_details=sql_header+"""
        WITH DATA AS (
            SELECT 
                    QUERY_PARAMETERIZED_HASH ,
                    SUM(QUERY_LOAD_PERCENT) QUERY_LOAD_PERCENT
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY                    
            WHERE TO_DATE(START_TIME) >= DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))    
                AND WAREHOUSE_NAME='"""+warehouse_name+"""'
                AND QUERY_PARAMETERIZED_HASH IS NOT NULL
                AND QUERY_LOAD_PERCENT IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC 
            LIMIT 100
        )
        SELECT DISTINCT QUERY_PARAMETERIZED_HASH
        FROM DATA
        """

        cur_details = conn.cursor()
        cur_details.execute(sql_query_details)
        
        column_count=cur_details.rowcount
        if int(cur_details.rowcount)!=0:
            headers =[i[0] for i in cur_details] 
            headers.insert(0,'MONTH')     
            html_file=html_file+str(headers).replace('"','')+", \n"
            
            sql_query_details=sql_header+"""
            WITH DATA AS (
                SELECT 
                    DATE_TRUNC('MONTH',start_time::TIMESTAMP_NTZ)               AS MONTH 
                    ,QUERY_PARAMETERIZED_HASH                                   AS QUERY_PARAMETERIZED_HASH
                    ,ROUND(SUM(NVL(QUERY_LOAD_PERCENT,0)),2)                    AS QUERY_LOAD_PERCENT
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY                    
                WHERE TO_DATE(START_TIME) >= DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))    
                    AND WAREHOUSE_NAME='"""+warehouse_name+"""'
                    AND QUERY_PARAMETERIZED_HASH IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'MONTH',","")+""") 
                GROUP BY 1,2
                ORDER BY 3 DESC 
            )
            , DATA_PIVOT AS (
                SELECT *
                FROM DATA
                    PIVOT(  SUM (QUERY_LOAD_PERCENT)  FOR QUERY_PARAMETERIZED_HASH IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'MONTH',","")+""") DEFAULT ON NULL (0) )  
                ORDER BY MONTH
            )
            SELECT ARRAY_CONSTRUCT(*) AS DATA  FROM DATA_PIVOT;
            """
            
            cur_details = conn.cursor()
            cur_details.execute(sql_query_details)

            if int(cur_details.rowcount)!=0:
                counter=0
                for (row_data) in cur_details:
                    counter=counter+1
                    if counter==cur_details.rowcount:
                        html_file=html_file+str(row_data[0])
                    else:
                        html_file=html_file+str(row_data[0])+""","""

            html_file=html_file+html_body1
        
            for i in range(1, column_count+1):
                if i==column_count:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""
                
            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Load details per query for warehouse """+warehouse_name+""" per month`,
            """+html_bar_month_tail

            create_output_file('per_month_load_details_for_warehouse_'+warehouse_name.lower()+'.html',html_file)
            report_sections["A - Computing"].update({'per_month_load_details_for_warehouse_'+warehouse_name.lower()+'.html':'bar'})
            
    except Exception as error:
        print("[bar_month_load_details_by_warehouse]: An exception occurred:", error)
            
def bar_week_load_details_by_warehouse(conn,warehouse_name):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn             
        html_file=html_header
        
        sql_query_details=sql_header+"""
        WITH DATA AS (
            SELECT 
                    QUERY_PARAMETERIZED_HASH ,
                    SUM(QUERY_LOAD_PERCENT) QUERY_LOAD_PERCENT
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY                    
            WHERE TO_DATE(START_TIME) >= DATEADD(DAY,-7,TO_TIMESTAMP("""+report_formatted_time+"""))    
                AND WAREHOUSE_NAME='"""+warehouse_name+"""'
                AND QUERY_PARAMETERIZED_HASH IS NOT NULL
                AND QUERY_LOAD_PERCENT IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC 
            LIMIT 100
        )
        SELECT DISTINCT QUERY_PARAMETERIZED_HASH
        FROM DATA
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query_details)

        column_count=cur_details.rowcount
        if int(cur_details.rowcount)!=0:
            headers =[i[0] for i in cur_details]   
            headers.insert(0,'DATE')   
            html_file=html_file+str(headers).replace('"','')+", \n"

            sql_query_details=sql_header+"""
            WITH DATA AS (
                SELECT 
                    DATE_TRUNC('DAY',START_TIME::TIMESTAMP_NTZ)     AS DATE 
                    ,QUERY_PARAMETERIZED_HASH                       AS QUERY_PARAMETERIZED_HASH
                    ,ROUND(SUM(NVL(QUERY_LOAD_PERCENT,0)),2)        AS QUERY_LOAD_PERCENT
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY                    
                WHERE TO_DATE(START_TIME) >= DATEADD(DAY,-7,TO_TIMESTAMP("""+report_formatted_time+"""))    
                AND WAREHOUSE_NAME='"""+warehouse_name+"""'
                AND QUERY_PARAMETERIZED_HASH IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""")
                GROUP BY 1,2
                ORDER BY 3 DESC 
            )
            , DATA_PIVOT AS (
            SELECT *
            FROM DATA
                PIVOT(  SUM (QUERY_LOAD_PERCENT)  FOR QUERY_PARAMETERIZED_HASH IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'MONTH',","")+""") DEFAULT ON NULL (0) )  
            ORDER BY DATE
            )
            SELECT ARRAY_CONSTRUCT(*) AS DATA  FROM DATA_PIVOT;
            """
            cur_details = conn.cursor()
            cur_details.execute(sql_query_details)

            if int(cur_details.rowcount)!=0:
                counter=0
                for (row_data) in cur_details:
                    counter=counter+1
                    if counter==cur_details.rowcount:
                        html_file=html_file+str(row_data[0])
                    else:
                        html_file=html_file+str(row_data[0])+""","""

            html_file=html_file+html_body1
        
            for i in range(1, column_count+1):
                if i==column_count:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""
                
            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Load details per query for warehouse """+warehouse_name+""" for last week`,
            """+html_bar_day_tail
    
            create_output_file('last_week_load_details_for_warehouse_'+warehouse_name.lower()+'.html',html_file)
            report_sections["A - Computing"].update({'last_week_load_details_for_warehouse_'+warehouse_name.lower()+'.html':'bar'})
    
    except Exception as error:
        print("[bar_week_load_details_by_warehouse]: An exception occurred:", error)
            
def line_history_daily_credits_used_by_service(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        
        iterations = ["CREDITS_USED_COMPUTE", "CREDITS_USED_CLOUD_SERVICES","CREDITS_USED", "CREDITS_ADJUSTMENT_CLOUD_SERVICES","CREDITS_BILLED"]
        
        for iteration in iterations:
            
            html_file=html_header+"""
            ['DATE','AI_SERVICES','AUTO_CLUSTERING','COPY_FILES','HYBRID_TABLE_REQUESTS',
                'MATERIALIZED_VIEW','PIPE','QUERY_ACCELERATION','REPLICATION','SEARCH_OPTIMIZATION','SERVERLESS_TASK',
                'SNOWPARK_CONTAINER_SERVICES','SNOWPIPE_STREAMING','WAREHOUSE_METERING','WAREHOUSE_METERING_READER'] ,
            """
        
            sql_query_details=sql_header+"""
            WITH DATA AS (
                SELECT DATE_TRUNC('DAY',USAGE_DATE::TIMESTAMP_NTZ)              AS DATE 
                  ,SERVICE_TYPE                                                 AS SERVICE_TYPE
                  ,ROUND(SUM(NVL("""+iteration+""",0)),2)                       AS """+iteration+"""
                FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY                     
                WHERE TO_DATE(USAGE_DATE) >= DATEADD(month,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))    
                GROUP BY 1,2
                ORDER BY 1
            )
            , DATA_PIVOT AS (
            SELECT *
            FROM DATA
                PIVOT(  sum ("""+iteration+""") FOR SERVICE_TYPE IN ('AI_SERVICES','AUTO_CLUSTERING','COPY_FILES','HYBRID_TABLE_REQUESTS',
                'MATERIALIZED_VIEW','PIPE','QUERY_ACCELERATION','REPLICATION','SEARCH_OPTIMIZATION','SERVERLESS_TASK',
                'SNOWPARK_CONTAINER_SERVICES','SNOWPIPE_STREAMING','WAREHOUSE_METERING','WAREHOUSE_METERING_READER') DEFAULT ON NULL (0) ) 
            ORDER BY DATE
            )
            SELECT ARRAY_CONSTRUCT(*) AS DATA  FROM DATA_PIVOT;
            """
            
            cur_details = conn.cursor()
            cur_details.execute(sql_query_details)
        
            if int(cur_details.rowcount)!=0:
                counter=0
                for (row_data) in cur_details:
                    counter=counter+1
                    if counter==cur_details.rowcount:
                        html_file=html_file+str(row_data[0])
                    else:
                        html_file=html_file+str(row_data[0])+""","""

                html_file=html_file+html_body1+"""
                row[1],row[2],row[3],row[4],row[5],
                row[6],row[7],row[8],row[9],row[10],
                row[11],row[12],row[13],row[14] 
                """+html_body2+"""
                title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
                Chart Creation Date: """+report_formatted_time+"""
                Daily """+iteration+""" by service`,
                """+html_line_hour_tail
                
                create_output_file('daily_credits_used_for_'+iteration.lower()+'.html',html_file)
                report_sections["C - Credits"].update({'daily_credits_used_for_'+iteration.lower()+'.html':'line'})
            
    except Exception as error:
        print("[line_history_daily_credits_used_by_service]: An exception occurred:", error)
    
def bar_month_credits_used_by_service(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        
        iterations = ["CREDITS_USED_COMPUTE", "CREDITS_USED_CLOUD_SERVICES","CREDITS_USED", "CREDITS_ADJUSTMENT_CLOUD_SERVICES","CREDITS_BILLED"]
        
        for iteration in iterations:
            
            html_file=html_header+"""
            ['DATE','AI_SERVICES','AUTO_CLUSTERING','COPY_FILES','HYBRID_TABLE_REQUESTS',
                'MATERIALIZED_VIEW','PIPE','QUERY_ACCELERATION','REPLICATION','SEARCH_OPTIMIZATION','SERVERLESS_TASK',
                'SNOWPARK_CONTAINER_SERVICES','SNOWPIPE_STREAMING','WAREHOUSE_METERING','WAREHOUSE_METERING_READER'] ,
            """
        
            sql_query_details=sql_header+"""
            WITH DATA AS (
                SELECT DATE_TRUNC('MONTH',USAGE_DATE::TIMESTAMP_NTZ)            AS DATE 
                  ,SERVICE_TYPE                                                 AS SERVICE_TYPE
                  ,ROUND(SUM(NVL("""+iteration+""",0)),2)                       AS """+iteration+"""
                FROM snowflake.account_usage.METERING_DAILY_HISTORY                     
                WHERE TO_DATE(USAGE_DATE) >= DATEADD(month,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))    
                GROUP BY 1,2
                ORDER BY 1
            )
            , DATA_PIVOT AS (
            SELECT *
            FROM DATA
                PIVOT(  sum ("""+iteration+""") FOR SERVICE_TYPE IN ('AI_SERVICES','AUTO_CLUSTERING','COPY_FILES','HYBRID_TABLE_REQUESTS',
                'MATERIALIZED_VIEW','PIPE','QUERY_ACCELERATION','REPLICATION','SEARCH_OPTIMIZATION','SERVERLESS_TASK',
                'SNOWPARK_CONTAINER_SERVICES','SNOWPIPE_STREAMING','WAREHOUSE_METERING','WAREHOUSE_METERING_READER') DEFAULT ON NULL (0) ) 
            ORDER BY DATE
            )
            SELECT ARRAY_CONSTRUCT(*) AS DATA  FROM DATA_PIVOT;
            """
            
            cur_details = conn.cursor()
            cur_details.execute(sql_query_details)
        
            if int(cur_details.rowcount)!=0:
                counter=0
                for (row_data) in cur_details:
                    counter=counter+1
                    if counter==cur_details.rowcount:
                        html_file=html_file+str(row_data[0])
                    else:
                        html_file=html_file+str(row_data[0])+""","""

                html_file=html_file+html_body1+"""
                row[1],row[2],row[3],row[4],row[5],
                row[6],row[7],row[8],row[9],row[10],
                row[11],row[12],row[13],row[14]
                """+html_body2+"""
                title:  `Prismafy v1.0 - https://github.com/prismafy/prismafy
                Chart Creation Date: """+report_formatted_time+"""
                Daily """+iteration+""" by service for last month`,
                """+html_bar_month_tail
                
                create_output_file('per_month_credits_used_for_'+iteration.lower()+'.html',html_file)
                report_sections["C - Credits"].update({'per_month_credits_used_for_'+iteration.lower()+'.html':'bar'})
            
    except Exception as error:
        print("[bar_month_credits_used_by_service]: An exception occurred:", error)
    
def bar_week_credits_used_by_service(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        
        iterations = ["CREDITS_USED_COMPUTE", "CREDITS_USED_CLOUD_SERVICES","CREDITS_USED", "CREDITS_ADJUSTMENT_CLOUD_SERVICES","CREDITS_BILLED"]
        
        for iteration in iterations:
            
            html_file=html_header+"""
            ['DATE','AI_SERVICES','AUTO_CLUSTERING','COPY_FILES','HYBRID_TABLE_REQUESTS',
                'MATERIALIZED_VIEW','PIPE','QUERY_ACCELERATION','REPLICATION','SEARCH_OPTIMIZATION','SERVERLESS_TASK',
                'SNOWPARK_CONTAINER_SERVICES','SNOWPIPE_STREAMING','WAREHOUSE_METERING','WAREHOUSE_METERING_READER'] ,
            """
        
            sql_query_details=sql_header+"""
            WITH DATA AS (
                SELECT DATE_TRUNC('DAY',USAGE_DATE::TIMESTAMP_NTZ)            AS DATE 
                  ,SERVICE_TYPE                                                 AS SERVICE_TYPE
                  ,ROUND(SUM(NVL("""+iteration+""",0)),2)                       AS """+iteration+"""
                FROM snowflake.account_usage.METERING_DAILY_HISTORY                     
                WHERE TO_DATE(USAGE_DATE) >= DATEADD(DAY,-7,TO_TIMESTAMP("""+report_formatted_time+"""))    
                GROUP BY 1,2
                ORDER BY 1
            )
            , DATA_PIVOT AS (
            SELECT *
            FROM DATA
                PIVOT(  sum ("""+iteration+""") FOR SERVICE_TYPE IN ('AI_SERVICES','AUTO_CLUSTERING','COPY_FILES','HYBRID_TABLE_REQUESTS',
                'MATERIALIZED_VIEW','PIPE','QUERY_ACCELERATION','REPLICATION','SEARCH_OPTIMIZATION','SERVERLESS_TASK',
                'SNOWPARK_CONTAINER_SERVICES','SNOWPIPE_STREAMING','WAREHOUSE_METERING','WAREHOUSE_METERING_READER') DEFAULT ON NULL (0) ) 
            ORDER BY DATE
            )
            SELECT ARRAY_CONSTRUCT(*) AS DATA  FROM DATA_PIVOT;
            """
            
            cur_details = conn.cursor()
            cur_details.execute(sql_query_details)
        
            if int(cur_details.rowcount)!=0:
                counter=0
                for (row_data) in cur_details:
                    counter=counter+1
                    if counter==cur_details.rowcount:
                        html_file=html_file+str(row_data[0])
                    else:
                        html_file=html_file+str(row_data[0])+""","""
                        
                html_file=html_file+html_body1+"""
                row[1],row[2],row[3],row[4],row[5],
                row[6],row[7],row[8],row[9],row[10],
                row[11],row[12],row[13],row[14]"""+html_body2+"""
                title:  `Prismafy v1.0 - https://github.com/prismafy/prismafy
                Chart Creation Date: """+report_formatted_time+"""
                Daily """+iteration+""" by service for last week`,"""+html_bar_day_tail
                    
                create_output_file('last_week_credits_used_for_'+iteration.lower()+'.html',html_file)
                report_sections["C - Credits"].update({'last_week_credits_used_for_'+iteration.lower()+'.html':'bar'})
            
    except Exception as error:
        print("[bar_week_credits_used_by_service]: An exception occurred:", error)
 
def line_history_login_history(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
                        
        html_file=html_header+"""
        [
        'DATE',
        'COUNT_LOGINS'
        ],
        """
    
        sql_query_details=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('HOUR',EVENT_TIMESTAMP::TIMESTAMP_NTZ)         AS DATE, 
            COUNT(*)                                                         AS COUNT_LOGINS
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY   Q
        WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
        GROUP BY 1
        ORDER BY DATE  
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query_details)
        
        if int(cur_details.rowcount)!=0:
            for (row_data) in cur_details:
                html_file=html_file+str(row_data[0])+""","""
            
            html_file=html_file+html_body1+"""
    	    row[1]"""+html_body2+"""
    		title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            History Count Logins`,"""+html_line_hour_tail
    
            create_output_file('history_logins.html',html_file)
            report_sections["D - Performance"].update({'history_logins.html':'line'})
            
    except Exception as error:
        print("[line_history_login_history]: An exception occurred:", error)
 
def line_month_top_logins_by_users(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        html_file=html_header
        
        sql_query=sql_header+"""
        WITH DATA AS ( 
            SELECT DATE_TRUNC('DAY',EVENT_TIMESTAMP::TIMESTAMP_NTZ)              AS DATE, 
                USER_NAME                                                        AS USER_NAME,
                COUNT(*)                                                         AS COUNT_LOGINS
            FROM snowflake.account_usage.LOGIN_HISTORY   Q
            WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            GROUP BY 1,2
            ORDER BY 3 DESC 
            LIMIT 25
        )
        SELECT DISTINCT USER_NAME
        FROM  DATA
        """
    
        cur = conn.cursor()
        cur.execute(sql_query)
        
        column_count=cur.rowcount
        if int(cur.rowcount)!=0:
            headers =[i[0] for i in cur]  
            headers.insert(0,'DATE')  					
            html_file=html_file+str(headers).replace('"','')+", \n"   
        
            sql_query=sql_header+"""
            WITH DATA AS (
                SELECT DATE_TRUNC('DAY',EVENT_TIMESTAMP::TIMESTAMP_NTZ)              AS DATE, 
                    USER_NAME                                                        AS USER_NAME,
                    COUNT(*)                                                         AS COUNT_LOGINS
                FROM snowflake.account_usage.LOGIN_HISTORY   Q
                WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                GROUP BY 1,2
                ORDER BY 3 DESC  
                LIMIT 50
            )
            , DATA_PIVOT AS (
            SELECT *
            FROM  DATA
                PIVOT(  SUM (COUNT_LOGINS)  FOR USER_NAME IN ("""+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) )    
            ORDER BY DATE
            )
            SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA_PIVOT
            """

            html_file=html_header
                
            html_file=html_file+str(headers)+","
            
            cur = conn.cursor()
            cur.execute(sql_query)

            if int(cur.rowcount)!=0:
                for (row_data) in cur:
                    

                    if int(cur.rowcount)!=0:
                        counter=0
                        for (row_data) in cur:
                            counter=counter+1
                            if counter==cur.rowcount:
                                html_file=html_file+str(row_data[0])
                            else:
                                html_file=html_file+str(row_data[0])+""","""

            html_file=html_file+html_body1
            for i in range(1, column_count+1):
                if i==column_count:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""
            
            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Login history for top users`,"""+html_line_hour_tail
            
            create_output_file('history_top_logins_by_users.html',html_file)
            report_sections["E - Security"].update({'history_top_logins_by_users.html':'line'})
            
    except Exception as error:
        print("[line_month_top_logins_by_users]: An exception occurred:", error)

def line_history_login_by_status(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        
        html_file=html_header+"""
        [
        'DATE',
        'FAILED LOGINS',
        'SUCCEDED LOGINS'
        ],
        """
        
        sql_query_details=sql_header+"""
        WITH FAILED AS (
        SELECT 
            DATE_TRUNC('HOUR',EVENT_TIMESTAMP::TIMESTAMP_NTZ)   AS DATE, 
            'FAILED'                                            AS STATUS,
            COUNT(*)                                            AS COUNT_LOGINS
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY   Q
        WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
        AND IS_SUCCESS='NO'
        GROUP BY 1
        ORDER BY DATE  
        )
        , SUCCEDED AS (
            SELECT 
                DATE_TRUNC('HOUR',EVENT_TIMESTAMP::TIMESTAMP_NTZ)       AS DATE, 
                'SUCCEDED'                                              AS STATUS,
                COUNT(*)                                                AS COUNT_LOGINS
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY   Q
            WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND IS_SUCCESS='YES'
            GROUP BY 1
            ORDER BY DATE 
        )
        , DATA AS (
            SELECT DATE, STATUS, COUNT_LOGINS FROM FAILED
            UNION ALL
            SELECT DATE, STATUS, COUNT_LOGINS FROM SUCCEDED
            ORDER BY DATE
        )
        , DATA_PIVOT AS (
            SELECT * 
            FROM DATA
            PIVOT(  SUM (COUNT_LOGINS)  FOR STATUS IN ('FAILED','SUCCEDED') DEFAULT ON NULL (0) )    
            ORDER BY DATE
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA_PIVOT;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query_details)
        
        if int(cur_details.rowcount)!=0:
            counter=0
            for (row_data) in cur_details:
                counter=counter+1
                if counter==cur_details.rowcount:
                    html_file=html_file+str(row_data[0])+""","""
                else:
                    html_file=html_file+str(row_data[0])
            
            html_file=html_file+html_body1+"""
    	    row[1],row[2]"""+html_body2+"""
    		title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Login history by Status`,"""+html_line_hour_tail
            
            create_output_file('history_logins_by_status.html',html_file)
            report_sections["D - Performance"].update({'history_logins_by_status.html':'line'})
            
    except Exception as error:
        print("[line_history_login_by_status]: An exception occurred:", error)

def table_history_failed_login(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn        
        html_file=html_table_header
        
        sql_query=sql_header+"""
        SELECT 
            DATE_TRUNC('HOUR',EVENT_TIMESTAMP::TIMESTAMP_NTZ)   AS DATE, 
            USER_NAME                                           AS USER_NAME,
            CLIENT_IP                                           AS CLIENT_IP,
            REPORTED_CLIENT_TYPE                                AS REPORTED_CLIENT_TYPE,
            REPORTED_CLIENT_VERSION                             AS REPORTED_CLIENT_VERSION,
            FIRST_AUTHENTICATION_FACTOR                         AS FIRST_AUTHENTICATION_FACTOR,
            SECOND_AUTHENTICATION_FACTOR                        AS SECOND_AUTHENTICATION_FACTOR,
            ERROR_CODE                                          AS ERROR_CODE,
            ERROR_MESSAGE                                       AS ERROR_MESSAGE
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY   Q
            WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND IS_SUCCESS='NO'
            ORDER BY DATE 
            LIMIT 500
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>History of Failed Logins</h3>
        <table class="tabla1">
        <tr>
        <th >DATE</th>
        <th >USER_NAME</th>
        <th >CLIENT_IP</th>
        <th >REPORTED_CLIENT_TYPE</th>
        <th >REPORTED_CLIENT_VERSION</th>
        <th >FIRST_AUTHENTICATION_FACTOR</th>
        <th >SECOND_AUTHENTICATION_FACTOR</th>
        <th >ERROR_CODE</th>
        <th >ERROR_MESSAGE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_DATE, ROW_COUNT_LOGINS, ROW_CLIENT_IP, ROW_REPORTED_CLIENT_TYPE, ROW_REPORTED_CLIENT_VERSION, ROW_FIRST_AUTHENTICATION_FACTOR, ROW_SECOND_AUTHENTICATION_FACTOR, ROW_ERROR_CODE, ROW_ERROR_MESSAGE) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_DATE)+"""</td> 
                <td>"""+str(ROW_COUNT_LOGINS)+"""</td> 
                 <td>"""+str(ROW_CLIENT_IP)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_TYPE)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_VERSION)+"""</td> 
                 <td>"""+str(ROW_FIRST_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_SECOND_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_ERROR_CODE)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_ERROR_MESSAGE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
        
        create_output_file('history_failed_logins.html',html_file)
        report_sections["E - Security"].update({'history_failed_logins.html':'table'})

    
    except Exception as error:
        print("[table_history_failed_login]: An exception occurred:", error)

def table_month_new_login(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn        
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH HIST AS (
            SELECT 
                DISTINCT USER_NAME
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY   Q"""
        if int(months_history)<-2:
            sql_query=sql_query+""" WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))"""
        else:
            sql_query=sql_query+""" WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,-2,TO_TIMESTAMP("""+report_formatted_time+"""))"""

        sql_query=sql_query+""" AND TO_DATE(Q.EVENT_TIMESTAMP) < DATEADD(MONTH,-1,TO_TIMESTAMP("""+report_formatted_time+"""))
                AND IS_SUCCESS='YES'
            
        )
        , MONTH AS (
            SELECT 
                DATE_TRUNC('HOUR',EVENT_TIMESTAMP::TIMESTAMP_NTZ)   AS DATE, 
                USER_NAME                                           AS USER_NAME,
                CLIENT_IP                                           AS CLIENT_IP,
                REPORTED_CLIENT_TYPE                                AS REPORTED_CLIENT_TYPE,
                REPORTED_CLIENT_VERSION                             AS REPORTED_CLIENT_VERSION,
                FIRST_AUTHENTICATION_FACTOR                         AS FIRST_AUTHENTICATION_FACTOR,
                SECOND_AUTHENTICATION_FACTOR                        AS SECOND_AUTHENTICATION_FACTOR,
                ERROR_CODE                                          AS ERROR_CODE,
                ERROR_MESSAGE                                       AS ERROR_MESSAGE
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY   Q
            WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,-1,TO_TIMESTAMP("""+report_formatted_time+"""))
                AND IS_SUCCESS='YES'
            ORDER BY 1 
        )
        SELECT * FROM MONTH WHERE USER_NAME NOT IN (SELECT  USER_NAME FROM HIST) AND (SELECT  COUNT(*) FROM HIST)>0
        """
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>New Users with logins in the last month</h3>
        <table class="tabla1">
        <tr>
        <th >DATE</th>
        <th >USER_NAME</th>
        <th >CLIENT_IP</th>
        <th >REPORTED_CLIENT_TYPE</th>
        <th >REPORTED_CLIENT_VERSION</th>
        <th >FIRST_AUTHENTICATION_FACTOR</th>
        <th >SECOND_AUTHENTICATION_FACTOR</th>
        <th >ERROR_CODE</th>
        <th >ERROR_MESSAGE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_DATE, ROW_COUNT_LOGINS, ROW_CLIENT_IP, ROW_REPORTED_CLIENT_TYPE, ROW_REPORTED_CLIENT_VERSION, ROW_FIRST_AUTHENTICATION_FACTOR, ROW_SECOND_AUTHENTICATION_FACTOR, ROW_ERROR_CODE, ROW_ERROR_MESSAGE) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_DATE)+"""</td> 
                <td>"""+str(ROW_COUNT_LOGINS)+"""</td> 
                 <td>"""+str(ROW_CLIENT_IP)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_TYPE)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_VERSION)+"""</td> 
                 <td>"""+str(ROW_FIRST_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_SECOND_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_ERROR_CODE)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_ERROR_MESSAGE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
        
        create_output_file('last_month_new_logins.html',html_file)
        report_sections["E - Security"].update({'last_month_new_logins.html':'table'})

    
    except Exception as error:
        print("[table_month_new_login]: An exception occurred:", error)

def table_less_frequent_logins(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn        
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH HIST AS (
            SELECT 
                ROW_NUMBER() OVER (PARTITION BY  DATE_TRUNC('MONTH',Q.EVENT_TIMESTAMP) ORDER BY COUNT(*)  ASC)       AS TOP_N,
                DATE_TRUNC('MONTH',Q.EVENT_TIMESTAMP)   AS MONTH,
                USER_NAME                               AS USER_NAME,
                COUNT(*)                                AS COUNT_LOGINS
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY   Q
        """
        if int(months_history)<-2:
            sql_query=sql_query+""" WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))"""
        else:
            sql_query=sql_query+""" WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,-2,TO_TIMESTAMP("""+report_formatted_time+"""))"""
        sql_query=sql_query+"""
                AND IS_SUCCESS='YES'            
        GROUP BY 2,3
        ORDER BY 1,2
        )
        SELECT * FROM HIST 
        WHERE TOP_N<=10
        ORDER BY MONTH,TOP_N
        """
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Users with less frequent logins</h3>
        <table class="tabla1">
        <tr>
        <th >TOP_N</th>
        <th >MONTH</th>
        <th >CLIENT_IP</th>
        <th >COUNT_LOGINS</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_TOP_N, ROW_MONTH, ROW_CLIENT_IP, ROW_COUNT_LOGINS) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_TOP_N)+"""</td> 
                <td>"""+str(ROW_MONTH)+"""</td> 
                 <td>"""+str(ROW_CLIENT_IP)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_COUNT_LOGINS)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
        
        create_output_file('less_frequent_logins.html',html_file)
        report_sections["E - Security"].update({'less_frequent_logins.html':'table'})

    
    except Exception as error:
        print("[table_less_frequent_logins]: An exception occurred:", error)

def table_week_new_login(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn        
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH HIST AS (
            SELECT 
                DISTINCT USER_NAME
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY   Q"""
        if int(months_history)<-2:
            sql_query=sql_query+""" WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))"""
        else:
            sql_query=sql_query+""" WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,-2,TO_TIMESTAMP("""+report_formatted_time+"""))"""

        sql_query=sql_query+""" AND TO_DATE(Q.EVENT_TIMESTAMP) < DATEADD(DAY,-7,TO_TIMESTAMP("""+report_formatted_time+"""))
                AND IS_SUCCESS='YES'
            
        )
        , MONTH AS (
            SELECT 
                DATE_TRUNC('HOUR',EVENT_TIMESTAMP::TIMESTAMP_NTZ)   AS DATE, 
                USER_NAME                                           AS USER_NAME,
                CLIENT_IP                                           AS CLIENT_IP,
                REPORTED_CLIENT_TYPE                                AS REPORTED_CLIENT_TYPE,
                REPORTED_CLIENT_VERSION                             AS REPORTED_CLIENT_VERSION,
                FIRST_AUTHENTICATION_FACTOR                         AS FIRST_AUTHENTICATION_FACTOR,
                SECOND_AUTHENTICATION_FACTOR                        AS SECOND_AUTHENTICATION_FACTOR,
                ERROR_CODE                                          AS ERROR_CODE,
                ERROR_MESSAGE                                       AS ERROR_MESSAGE
            FROM snowflake.account_usage.LOGIN_HISTORY   Q
            WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(DAY,-7,TO_TIMESTAMP("""+report_formatted_time+"""))
                AND IS_SUCCESS='YES'
            ORDER BY DATE 
        )
        SELECT * FROM MONTH WHERE USER_NAME NOT IN (SELECT  USER_NAME FROM HIST) AND (SELECT  COUNT(*) FROM HIST)>0
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Users with new logins in the last week</h3>
        <table class="tabla1">
        <tr>
        <th >DATE</th>
        <th >USER_NAME</th>
        <th >CLIENT_IP</th>
        <th >REPORTED_CLIENT_TYPE</th>
        <th >REPORTED_CLIENT_VERSION</th>
        <th >FIRST_AUTHENTICATION_FACTOR</th>
        <th >SECOND_AUTHENTICATION_FACTOR</th>
        <th >ERROR_CODE</th>
        <th >ERROR_MESSAGE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_DATE, ROW_COUNT_LOGINS, ROW_CLIENT_IP, ROW_REPORTED_CLIENT_TYPE, ROW_REPORTED_CLIENT_VERSION, ROW_FIRST_AUTHENTICATION_FACTOR, ROW_SECOND_AUTHENTICATION_FACTOR, ROW_ERROR_CODE, ROW_ERROR_MESSAGE) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_DATE)+"""</td> 
                <td>"""+str(ROW_COUNT_LOGINS)+"""</td> 
                 <td>"""+str(ROW_CLIENT_IP)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_TYPE)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_VERSION)+"""</td> 
                 <td>"""+str(ROW_FIRST_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_SECOND_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_ERROR_CODE)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_ERROR_MESSAGE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
        
        create_output_file('last_week_new_logins.html',html_file)
        report_sections["E - Security"].update({'last_week_new_logins.html':'table'})

    
    except Exception as error:
        print("[table_week_new_login]: An exception occurred:", error)

def table_day_new_login(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn        
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH HIST AS (
            SELECT 
                DISTINCT USER_NAME
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY   Q"""
        if int(months_history)<-2:
            sql_query=sql_query+""" WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))"""
        else:
            sql_query=sql_query+""" WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(MONTH,-2,TO_TIMESTAMP("""+report_formatted_time+"""))"""

        sql_query=sql_query+""" AND TO_DATE(Q.EVENT_TIMESTAMP) < DATEADD(DAY,-1,TO_TIMESTAMP("""+report_formatted_time+"""))
                AND IS_SUCCESS='YES'
            
        )
        , MONTH AS (
            SELECT 
                DATE_TRUNC('HOUR',EVENT_TIMESTAMP::TIMESTAMP_NTZ)   AS DATE, 
                USER_NAME                                           AS USER_NAME,
                CLIENT_IP                                           AS CLIENT_IP,
                REPORTED_CLIENT_TYPE                                AS REPORTED_CLIENT_TYPE,
                REPORTED_CLIENT_VERSION                             AS REPORTED_CLIENT_VERSION,
                FIRST_AUTHENTICATION_FACTOR                         AS FIRST_AUTHENTICATION_FACTOR,
                SECOND_AUTHENTICATION_FACTOR                        AS SECOND_AUTHENTICATION_FACTOR,
                ERROR_CODE                                          AS ERROR_CODE,
                ERROR_MESSAGE                                       AS ERROR_MESSAGE
            FROM snowflake.account_usage.LOGIN_HISTORY   Q
            WHERE  TO_DATE(Q.EVENT_TIMESTAMP) > DATEADD(DAY,-1,TO_TIMESTAMP("""+report_formatted_time+"""))
                AND IS_SUCCESS='YES'
            ORDER BY DATE 
        )
        SELECT * FROM MONTH WHERE USER_NAME NOT IN (SELECT USER_NAME FROM HIST) AND (SELECT  COUNT(*) FROM HIST)>0
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Users with new logins in the last day</h3>
        <table class="tabla1">
        <tr>
        <th >DATE</th>
        <th >USER_NAME</th>
        <th >CLIENT_IP</th>
        <th >REPORTED_CLIENT_TYPE</th>
        <th >REPORTED_CLIENT_VERSION</th>
        <th >FIRST_AUTHENTICATION_FACTOR</th>
        <th >SECOND_AUTHENTICATION_FACTOR</th>
        <th >ERROR_CODE</th>
        <th >ERROR_MESSAGE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_DATE, ROW_COUNT_LOGINS, ROW_CLIENT_IP, ROW_REPORTED_CLIENT_TYPE, ROW_REPORTED_CLIENT_VERSION, ROW_FIRST_AUTHENTICATION_FACTOR, ROW_SECOND_AUTHENTICATION_FACTOR, ROW_ERROR_CODE, ROW_ERROR_MESSAGE) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_DATE)+"""</td> 
                <td>"""+str(ROW_COUNT_LOGINS)+"""</td> 
                 <td>"""+str(ROW_CLIENT_IP)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_TYPE)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_VERSION)+"""</td> 
                 <td>"""+str(ROW_FIRST_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_SECOND_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_ERROR_CODE)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_ERROR_MESSAGE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
        
        create_output_file('last_day_new_logins.html',html_file)
        report_sections["E - Security"].update({'last_day_new_logins.html':'table'})

    
    except Exception as error:
        print("[table_day_new_login]: An exception occurred:", error)

def table_history_top_tables_by_storage(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn        
        iterations = ["ACTIVE_BYTES", "TIME_TRAVEL_BYTES","FAILSAFE_BYTES", "RETAINED_FOR_CLONE_BYTES"]
        
        for iteration in iterations:
            
            html_file=html_table_header
            
            sql_query=sql_header+"""
            SELECT 
                ROW_NUMBER() over (order by """+ iteration +"""  DESC)  AS TOP_N,
                TABLE_CATALOG                                           AS TABLE_CATALOG,
                TABLE_SCHEMA                                            AS TABLE_SCHEMA,
                TABLE_NAME                                              AS TABLE_NAME,
                IS_TRANSIENT                                            AS IS_TRANSIENT,
                ROUND(NVL(ACTIVE_BYTES,0)/1024/1024/1024,2)             AS ACTIVE_GB,
                ROUND(NVL(TIME_TRAVEL_BYTES,0)/1024/1024/1024,2)        AS TIME_TRAVEL_GB,
                ROUND(NVL(FAILSAFE_BYTES,0)/1024/1024/1024,2)           AS FAILSAFE_GB,
                ROUND(NVL(RETAINED_FOR_CLONE_BYTES,0)/1024/1024/1024,2) AS RETAINED_FOR_CLONE_GB,
                TABLE_CREATED::TIMESTAMP_NTZ                            AS TABLE_CREATED
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS    T
            WHERE DELETED=FALSE
            ORDER BY TOP_N
            LIMIT 100
            """
            
            cur = conn.cursor()
            cur.execute(sql_query)
        
            html_file=html_file+"""
            <h3>Top tables by """+iteration.lower()+"""</h3>
            <table class="tabla1">
            <tr>
            <th >TOP_N</th>
            <th >TABLE_CATALOG</th>
            <th >TABLE_SCHEMA</th>
            <th >TABLE_NAME</th>
            <th >IS_TRANSIENT</th>
            <th >ACTIVE_GB</th>
            <th >TIME_TRAVEL_GB</th>
            <th >FAILSAFE_GB</th>
            <th >RETAINED_FOR_CLONE_GB</th>
            <th >TABLE_CREATED</th>
            """
        
            if int(cur.rowcount)!=0:
                for (ROW_TOP_N, ROW_TABLE_CATALOG, ROW_TABLE_SCHEMA, ROW_TABLE_NAME, ROW_IS_TRANSIENT, ROW_ACTIVE_GB, ROW_TIME_TRAVEL_GB, ROW_FAILSAFE_GB, ROW_RETAINED_FOR_CLONE_GB, ROW_TABLE_CREATED) in cur:
                    html_file=html_file+""" <tr> 
                    <td>"""+str(ROW_TOP_N)+"""</td> 
                    <td>"""+str(ROW_TABLE_CATALOG)+"""</td> 
                    <td>"""+str(ROW_TABLE_SCHEMA)+"""</td> 
                     <td>"""+str(ROW_TABLE_NAME)+"""</td> 
                     <td>"""+str(ROW_IS_TRANSIENT)+"""</td> 
                     <td>"""+str(ROW_ACTIVE_GB)+"""</td> 
                     <td>"""+str(ROW_TIME_TRAVEL_GB)+"""</td> 
                     <td>"""+str(ROW_FAILSAFE_GB)+"""</td> 
                     <td>"""+str(ROW_RETAINED_FOR_CLONE_GB)+"""</td> 
                     <td class="cell_grow">"""+str(ROW_TABLE_CREATED)+"""</td> 
                    </tr> """
                
                html_file=html_file+html_table_tail
        
            create_output_file('history_top_tables_by_'+iteration.lower()+'.html',html_file)
            report_sections["B - Storage"].update({'history_top_tables_by_'+iteration.lower()+'.html':'table'})
    
    except Exception as error:
        print("[table_history_top_tables_storage]: An exception occurred:", error)
        
def table_history_top_table_by_pruning_efficiency(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn
        html_file=html_table_header
        
        sql_query=sql_header+"""
        SELECT 
            ROW_NUMBER() over (order by ((PARTITIONS_PRUNED/(PARTITIONS_SCANNED+PARTITIONS_PRUNED))*100)  ASC, (ROWS_SCANNED+ROWS_PRUNED) DESC ) AS TOP_N,
            START_TIME::TIMESTAMP_NTZ                               AS DATE,
            DATABASE_NAME                                           AS DATABASE_NAME,
            SCHEMA_NAME                                             AS SCHEMA_NAME,
            TABLE_NAME                                              AS TABLE_NAME,
            NUM_SCANS                                               AS NUM_SCANS,
            ROUND(NVL(PARTITIONS_SCANNED,0),2)                      AS PARTITIONS_SCANNED,
            ROUND(NVL(PARTITIONS_PRUNED,0),2)                       AS PARTITIONS_PRUNED,
            ROUND(NVL(ROWS_SCANNED,0),2)                            AS ROWS_SCANNED,
            ROUND(NVL(ROWS_PRUNED,0),2)                             AS ROWS_PRUNED,
            ROUND(((PARTITIONS_PRUNED/(PARTITIONS_SCANNED+PARTITIONS_PRUNED))*100),2)     AS PRUNING_EFFICIENCY_PERCENTAGE
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_PRUNING_HISTORY     T
        WHERE  TO_DATE(T.start_time) > DATEADD(MONTH,-1,TO_TIMESTAMP("""+report_formatted_time+"""))
        AND PARTITIONS_SCANNED>0
        AND ROWS_SCANNED>5000
        ORDER BY TOP_N
        LIMIT 200
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Top tables by pruning efficiency</h3>
        <table class="tabla2">
        <tr>
        <th >TOP_N</th>
        <th >DATE</th>
        <th >DATABASE_NAME</th>
        <th >SCHEMA_NAME</th>
        <th >TABLE_NAME</th>
        <th >NUM_SCANS</th>
        <th >PARTITIONS_SCANNED</th>
        <th >PARTITIONS_PRUNED</th>
        <th >ROWS_SCANNED</th>
        <th >ROWS_PRUNED</th>
        <th >PRUNING_EFFICIENCY_PERCENTAGE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_TOP_N, ROW_DATE,ROW_DATABASE_NAME, ROW_SCHEMA_NAME, ROW_TABLE_NAME, ROW_NUM_SCANS, ROW_PARTITIONS_SCANNED, ROW_PARTITIONS_PRUNED, ROW_ROWS_SCANNED, ROW_ROWS_PRUNED, ROW_PRUNING_EFFICIENCY_PERCENTAGE) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_TOP_N)+"""</td> 
                <td>"""+str(ROW_DATE)+"""</td> 
                <td>"""+str(ROW_DATABASE_NAME)+"""</td> 
                <td>"""+str(ROW_SCHEMA_NAME)+"""</td> 
                 <td>"""+str(ROW_TABLE_NAME)+"""</td> 
                 <td>"""+str(ROW_NUM_SCANS)+"""</td> 
                 <td>"""+str(ROW_PARTITIONS_SCANNED)+"""</td> 
                 <td>"""+str(ROW_PARTITIONS_PRUNED)+"""</td> 
                 <td>"""+str(ROW_ROWS_SCANNED)+"""</td> 
                 <td>"""+str(ROW_ROWS_PRUNED)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_PRUNING_EFFICIENCY_PERCENTAGE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('top_tables_by_pruning_efficiency.html',html_file)
        report_sections["D - Performance"].update({'top_tables_by_pruning_efficiency.html':'table'})
    
    except Exception as error:
        print("[table_history_table_pruning_efficiency]: An exception occurred:", error)
        
def line_history_pruning_efficiency_by_table(conn,database_name,schema_name,table_name):
    try:    
        global snowflake_conn
        global html_body1
        global html_line_hour_tail
        global report_sections
        conn =snowflake_conn
        
        if 'pruning_efficiency_for_table_'+  str(database_name).lower()+"_"+str(schema_name).lower()+"_"+str(table_name).lower()  +'.html' not in report_sections["D - Performance"]:
                
            html_file=html_header+"""
            [
            'DATE',
            'PARTITIONS_SCANNED',
            'PARTITIONS_PRUNED',
            'PARTITIONS_TOTAL'
            ],
            """
            sql_query_details=sql_header+"""
            WITH DATA AS (
                SELECT 
                    DATE_TRUNC('HOUR',START_TIME::TIMESTAMP_NTZ)                                AS DATE,
                    ROUND(NVL(SUM(PARTITIONS_SCANNED),0),2)                                     AS PARTITIONS_SCANNED,
                    ROUND(NVL(SUM(PARTITIONS_PRUNED),0),2)                                      AS PARTITIONS_PRUNED,
                    ROUND(NVL(SUM(PARTITIONS_SCANNED+PARTITIONS_PRUNED),0),2)                   AS PARTITIONS_TOTAL,
                FROM snowflake.account_usage.TABLE_PRUNING_HISTORY     T
                WHERE  TO_DATE(T.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                    AND DATABASE_NAME='"""+str(database_name).upper()+"""'
                    and SCHEMA_NAME='"""+str(schema_name).upper()+"""'
                    and TABLE_NAME='"""+str(table_name).upper()+"""'
                GROUP BY 1
            )
            SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA ORDER BY DATE;
            """
            cur_details = conn.cursor()
            cur_details.execute(sql_query_details)
            
            if int(cur_details.rowcount)!=0:
                for (row_data) in cur_details:
                    html_file=html_file+str(row_data[0])+""","""
                
                html_file=html_file+html_body1+"""
                row[1],row[2],row[3]
                """+html_body2+"""
                title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
                Chart Creation Date: """+report_formatted_time+"""
                Pruning efficiency for table """+str(database_name)+"."+str(schema_name)+"."+str(table_name)+"""`,
                """+html_line_hour_tail
            
                create_output_file('pruning_efficiency_for_table_'+  str(database_name).lower()+"_"+str(schema_name).lower()+"_"+str(table_name).lower()  +'.html',html_file)
                report_sections["D - Performance"].update({'pruning_efficiency_for_table_'+  str(database_name).lower()+"_"+str(schema_name).lower()+"_"+str(table_name).lower()  +'.html':'line'})
        
    except Exception as error:
        print("[line_history_pruning_efficiency_by_table]: An exception occurred:", error)  

def table_history_top_table_by_reclustering(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        conn =snowflake_conn
        global report_sections
        html_file=html_table_header
        
        sql_query=sql_header+"""
        SELECT 
            ROW_NUMBER() over (order by NUM_ROWS_RECLUSTERED DESC ) AS TOP_N,
            START_TIME::TIMESTAMP_NTZ                               AS DATE,
            DATABASE_NAME                                           AS DATABASE_NAME,
            SCHEMA_NAME                                             AS SCHEMA_NAME,
            TABLE_NAME                                              AS TABLE_NAME,
            ROUND(NVL(CREDITS_USED,0),2)                            AS CREDITS_USED,
            ROUND(NVL(NUM_BYTES_RECLUSTERED,0),2)                   AS NUM_BYTES_RECLUSTERED,
            ROUND(NVL(NUM_ROWS_RECLUSTERED,0),2)                    AS NUM_ROWS_RECLUSTERED,
        FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY T
        WHERE  TO_DATE(T.START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
        AND NUM_ROWS_RECLUSTERED>0
        ORDER BY TOP_N
        LIMIT 100
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Top table by reclustering</h3>
        <table class="tabla2">
        <tr>
        <th >TOP_N</th>
        <th >DATE</th>
        <th >DATABASE_NAME</th>
        <th >SCHEMA_NAME</th>
        <th >TABLE_NAME</th>
        <th >CREDITS_USED</th>
        <th >NUM_BYTES_RECLUSTERED</th>
        <th >NUM_ROWS_RECLUSTERED</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_TOP_N, ROW_DATE,ROW_DATABASE_NAME, ROW_SCHEMA_NAME, ROW_TABLE_NAME, ROW_CREDITS_USED, ROW_NUM_BYTES_RECLUSTERED, ROW_NUM_ROWS_RECLUSTERED) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_TOP_N)+"""</td> 
                <td>"""+str(ROW_DATE)+"""</td> 
                <td>"""+str(ROW_DATABASE_NAME)+"""</td> 
                <td>"""+str(ROW_SCHEMA_NAME)+"""</td> 
                 <td>"""+str(ROW_TABLE_NAME)+"""</td> 
                 <td>"""+str(ROW_CREDITS_USED)+"""</td> 
                 <td>"""+str(ROW_NUM_BYTES_RECLUSTERED)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_NUM_ROWS_RECLUSTERED)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
            create_output_file('history_top_table_by_reclustering.html',html_file)
            report_sections["D - Performance"].update({'history_top_table_by_reclustering.html':'table'})
    
    except Exception as error:
        print("[table_history_top_table_by_reclustering]: An exception occurred:", error)
        
def table_history_top_cloud_data_transfer(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail     
        global report_sections   
        conn =snowflake_conn        
        html_file=html_table_header
        
        sql_query=sql_header+"""
        SELECT 
            ROW_NUMBER() over (order by BYTES_TRANSFERRED DESC )    AS TOP_N,
            START_TIME::TIMESTAMP_NTZ                               AS DATE,
            SOURCE_CLOUD                                            AS SOURCE_CLOUD,
            SOURCE_REGION                                           AS SOURCE_REGION,
            TARGET_CLOUD                                            AS TARGET_CLOUD,
            TARGET_REGION                                           AS TARGET_REGION,
            ROUND(BYTES_TRANSFERRED/1024/1024/1024,2)               AS GB_TRANSFERRED,
            TRANSFER_TYPE                                           AS TRANSFER_TYPE,
        FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY T
        WHERE TO_DATE(T.START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
        ORDER BY TOP_N ASC
        LIMIT 100
        ;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Top clouds by data transfer</h3>
        <table class="tabla1">
        <tr>
        <th >TOP_N</th>
        <th >DATE</th>
        <th >SOURCE_CLOUD</th>
        <th >SOURCE_REGION</th>
        <th >TARGET_CLOUD</th>
        <th >TARGET_REGION</th>
        <th >GB_TRANSFERRED</th>
        <th >TRANSFER_TYPE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_TOP_N, ROW_DATE, ROW_SOURCE_CLOUD, ROW_SOURCE_REGION, ROW_TARGET_CLOUD, ROW_TARGET_REGION, ROW_GB_TRANSFERRED, ROW_TRANSFER_TYPE) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_TOP_N)+"""</td> 
                <td>"""+str(ROW_DATE)+"""</td> 
                <td>"""+str(ROW_SOURCE_CLOUD)+"""</td> 
                <td>"""+str(ROW_SOURCE_REGION)+"""</td> 
                 <td>"""+str(ROW_TARGET_CLOUD)+"""</td> 
                 <td>"""+str(ROW_TARGET_REGION)+"""</td> 
                 <td>"""+str(ROW_GB_TRANSFERRED)+"""</td> 
                 <td>"""+str(ROW_TRANSFER_TYPE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('history_top_clouds_by_data_transfer.html',html_file)
        report_sections["F - Data Transfer"].update({'history_top_clouds_by_data_transfer.html':'table'})
    
    except Exception as error:
        print("[table_history_top_cloud_data_transfer]: An exception occurred:", error)
        
def table_history_less_accessed_objects(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail     
        global report_sections   
        conn =snowflake_conn
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH HIST_DATA AS (
            SELECT
                T.QUERY_START_TIME AS DATE,
                R.VALUE:"objectName" AS OBJECT_NAME,
                R.VALUE:"objectDomain" AS OBJECT_TYPE
            FROM
                (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY) T,
                LATERAL FLATTEN(INPUT => T.BASE_OBJECTS_ACCESSED) R
            UNION ALL
            SELECT
                T.QUERY_START_TIME AS DATE,
                W.VALUE:"objectName" AS OBJECT_NAME,
                W.VALUE:"objectDomain" AS OBJECT_TYPE
            FROM
                (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY) T,
                LATERAL FLATTEN(INPUT => T.OBJECTS_MODIFIED) W
            UNION ALL
            SELECT
                T.QUERY_START_TIME AS DATE,
                D.VALUE:"objectName" AS OBJECT_NAME,
                D.VALUE:"objectDomain" AS OBJECT_TYPE
            FROM
                (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY) T,
                LATERAL FLATTEN(INPUT => T.direct_objects_accessed) D
        )
        , DATA AS (
        SELECT 
            STRTOK(OBJECT_NAME, '.', 1)     AS DATABASE_NAME, 
            STRTOK(OBJECT_NAME, '.', 2)     AS SCHEMA_NAME, 
            STRTOK(OBJECT_NAME, '.', 3)     AS OBJECT_NAME, 
            REPLACE(OBJECT_TYPE,'"')        AS OBJECT_TYPE, 
            MAX(DATE)                       AS DATE
        FROM HIST_DATA
        GROUP BY 1,2 ,3,4
        )
        SELECT 
            ROW_NUMBER() over (order by DATE ASC ) AS TOP_N,
            NVL(DATABASE_NAME,'')           AS DATABASE_NAME, 
            NVL(SCHEMA_NAME,'')             AS SCHEMA_NAME, 
            NVL(OBJECT_NAME,'')             AS OBJECT_NAME, 
            NVL(OBJECT_TYPE,'')             AS OBJECT_TYPE, 
            DATE                            AS DATE
        FROM DATA
        ORDER BY TOP_N ASC
        LIMIT 100
        ;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Less accessed objects</h3>
        <table class="tabla2">
        <tr>
        <th >TOP_N</th>
        <th >DATABASE_NAME</th>
        <th >SCHEMA_NAME</th>
        <th >OBJECT_NAME</th>
        <th >OBJECT_TYPE</th>
        <th >LAST_USED_DATE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_TOP_N, ROW_DATABASE_NAME,ROW_SCHEMA_NAME, ROW_OBJECT_NAME, ROW_OBJECT_TYPE, ROW_DATE) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_TOP_N)+"""</td> 
                <td>"""+str(ROW_DATABASE_NAME)+"""</td> 
                <td>"""+str(ROW_SCHEMA_NAME)+"""</td> 
                <td>"""+str(ROW_OBJECT_NAME)+"""</td> 
                 <td>"""+str(ROW_OBJECT_TYPE)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_DATE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('history_less_accessed_objects.html',html_file)
        report_sections["G - Maintenance"].update({'history_less_accessed_objects.html':'table'})
    
    except Exception as error:
        print("[table_history_less_accessed_objects]: An exception occurred:", error)
        
def line_history_data_transfer_by_cloud(conn):

    try:                
        global snowflake_conn
        global report_sections
        conn =snowflake_conn                
        
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT 
                ROW_NUMBER() over (order by BYTES_TRANSFERRED DESC )    AS TOP_N,
                START_TIME::TIMESTAMP_NTZ                               AS DATE,
                SOURCE_CLOUD                                            AS SOURCE_CLOUD,
                SOURCE_REGION                                           AS SOURCE_REGION,
                TARGET_CLOUD                                            AS TARGET_CLOUD,
                TARGET_REGION                                           AS TARGET_REGION,
                ROUND(BYTES_TRANSFERRED/1024/1024/1024,2)               AS GB_TRANSFERRED,
                TRANSFER_TYPE                                           AS TRANSFER_TYPE,
            FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY T
            WHERE TO_DATE(T.START_TIME) > DATEADD(MONTH,-1,TO_TIMESTAMP("""+report_formatted_time+"""))
            ORDER BY TOP_N ASC
            LIMIT 100
        )
            SELECT DISTINCT 
                SOURCE_CLOUD,SOURCE_REGION,TARGET_CLOUD,TARGET_REGION
            FROM DATA;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
        
        html_file=""
        if int(cur.rowcount)!=0:
            for (ROW_SOURCE_CLOUD,ROW_SOURCE_REGION,ROW_TARGET_CLOUD,ROW_TARGET_REGION) in cur:
                
                html_file=html_header+"""
                [
                'DATE',
                'GB_TRANSFERRED'
                ],
                """
        
                sql_query_details=sql_header+"""
                WITH DATA AS (
                SELECT 
                    DATE_TRUNC('HOUR',START_TIME::TIMESTAMP_NTZ)            AS DATE,
                    ROUND(SUM(BYTES_TRANSFERRED)/1024/1024/1024,2)          AS GB_TRANSFERRED
                FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY T
                WHERE TO_DATE(T.START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                  and SOURCE_CLOUD='"""+str(ROW_SOURCE_CLOUD)+"""'
                  and SOURCE_REGION='"""+str(ROW_SOURCE_REGION)+"""'
                  and TARGET_CLOUD='"""+str(ROW_TARGET_CLOUD)+"""'
                  and TARGET_REGION='"""+str(ROW_TARGET_REGION)+"""'
                GROUP BY 1
                ORDER BY 1  
                )
                SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA;
                """
                cur_details = conn.cursor()
                cur_details.execute(sql_query_details)
                
                if int(cur_details.rowcount)!=0:
                    for (row_data) in cur_details:
                        html_file=html_file+str(row_data[0])+""","""
                    
                    html_file=html_file+html_body1+"""
                    row[1]
                    """+html_body2+"""
                    title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
                    Chart Creation Date: """+report_formatted_time+"""
                    Data Transfer from """+  ROW_SOURCE_CLOUD +'_'+ROW_SOURCE_REGION+' to '+ROW_TARGET_CLOUD+'_'+ROW_TARGET_REGION+"""`,"""+html_line_hour_tail
                
                    create_output_file('history_data_transfer_per_cloud_'+  ROW_SOURCE_CLOUD.lower() +'_'+ROW_SOURCE_REGION.lower()+'_'+ROW_TARGET_CLOUD.lower()+'_'+ROW_TARGET_REGION.lower()+'.html',html_file)
                    report_sections["F - Data Transfer"].update({'history_data_transfer_per_cloud_'+  ROW_SOURCE_CLOUD.lower() +'_'+ROW_SOURCE_REGION.lower()+'_'+ROW_TARGET_CLOUD.lower()+'_'+ROW_TARGET_REGION.lower()+'.html':'line'})
        
    except Exception as error:
        print("[line_history_data_transfer_per_cloud]: An exception occurred:", error)
        
def table_history_users_with_highest_privileges(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail    
        global report_sections    
        conn =snowflake_conn        
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA  AS (
            SELECT 
                CREATED_ON::TIMESTAMP_NTZ       AS DATE,
                GRANTEE_NAME                    AS CHILD, 
                ROLE                            AS PARENT
                FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS 
            UNION ALL 
            SELECT 
                CREATED_ON::TIMESTAMP_NTZ       AS DATE,
                GRANTEE_NAME                    AS CHILD,
                NAME                            AS PARENT
            FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES 
                WHERE DELETED_ON IS NULL
                AND GRANTED_TO='ROLE'
                AND GRANTED_ON='ROLE'
                AND GRANTEE_NAME!='ACCOUNTADMIN'
            UNION ALL
            SELECT NULL,'ACCOUNTADMIN',NULL FROM DUAL
        ) 
        SELECT DATE,CHILD RECEIVER,SYS_CONNECT_BY_PATH(CHILD, ' -> ') GRANTS_PATH
        FROM DATA
        START WITH CHILD IN ('ACCOUNTADMIN','SECURITYADMIN','ORGADMIN')
        CONNECT BY PARENT= PRIOR CHILD
        ORDER BY 3,1;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Users with highest privileges</h3>
        <table class="tabla1">
        <tr>
        <th >DATE</th>
        <th >GRANTEE</th>
        <th >GRANTS_PATH</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_DATE, ROW_GRANTEE, ROW_GRANTS_PATH) in cur:
                html_file=html_file+""" <tr> """
                if str(ROW_DATE)=='None':
                    html_file=html_file+"""<td>Origin</td> """
                else:
                    html_file=html_file+"""<td>"""+str(ROW_DATE)+"""</td> """
                html_file=html_file+"""<td>"""+str(ROW_GRANTEE)+"""</td> 
                <td class="cell_grow">"""+str(ROW_GRANTS_PATH)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('users_with_highest_privileges.html',html_file)
        report_sections["E - Security"].update({'users_with_highest_privileges.html':'table'})

    
    except Exception as error:
        print("[table_history_users_with_highest_privileges]: An exception occurred:", error)
        
def table_history_top_database_by_storage(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
            WITH MAX_DATE AS (
                SELECT MAX (USAGE_DATE) USAGE_DATE, DATABASE_NAME 
                FROM snowflake.account_usage.DATABASE_STORAGE_USAGE_HISTORY
                WHERE USAGE_DATE > DATEADD(MONTH,-1,TO_TIMESTAMP("""+report_formatted_time+"""))
                GROUP BY 2
            )
            ,DATA AS (
              SELECT 
              Q.DATABASE_NAME                                              AS DATABASE_NAME,
              ROUND(AVERAGE_DATABASE_BYTES/1024/1024/1024,2)               AS AVERAGE_DATABASE_GB,
              ROUND(AVERAGE_FAILSAFE_BYTES/1024/1024/1024,2)               AS AVERAGE_FAILSAFE_GB,
              ROUND(AVERAGE_HYBRID_TABLE_STORAGE_BYTES/1024/1024/1024  ,2) AS AVERAGE_HYBRID_TABLE_STORAGE_GB
            FROM snowflake.account_usage.DATABASE_STORAGE_USAGE_HISTORY  Q
            INNER JOIN MAX_DATE
                ON (
                    MAX_DATE.DATABASE_NAME=Q.DATABASE_NAME 
                    AND Q.USAGE_DATE=MAX_DATE.USAGE_DATE
                    )
            ORDER BY AVERAGE_DATABASE_GB desc
            LIMIT 25
            )
            SELECT 
                ROW_NUMBER() over (order by AVERAGE_DATABASE_GB  DESC)       AS TOP_N
                ,DATABASE_NAME
                ,AVERAGE_DATABASE_GB
                ,AVERAGE_FAILSAFE_GB
                ,AVERAGE_HYBRID_TABLE_STORAGE_GB
            FROM DATA
            ORDER BY TOP_N;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Top database by storage</h3>
        <table class="tabla1">
        <tr>
        <th >TOP_N</th>
        <th >DATABASE_NAME</th>
        <th >AVERAGE_DATABASE_GB</th>
        <th >AVERAGE_FAILSAFE_GB</th>
        <th >AVERAGE_HYBRID_TABLE_STORAGE_GB</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_TOP_N, ROW_DATABASE_NAME, ROW_AVERAGE_DATABASE_GB, ROW_AVERAGE_FAILSAFE_GB, ROW_AVERAGE_HYBRID_TABLE_STORAGE_GB) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_TOP_N)+"""</td> 
                <td>"""+str(ROW_DATABASE_NAME)+"""</td> 
                 <td>"""+str(ROW_AVERAGE_DATABASE_GB)+"""</td> 
                 <td>"""+str(ROW_AVERAGE_FAILSAFE_GB)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_AVERAGE_HYBRID_TABLE_STORAGE_GB)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('top_database_by_storage.html',html_file)
        report_sections["B - Storage"].update({'top_database_by_storage.html':'table'})

    
    except Exception as error:
        print("[table_history_top_database_by_storage]: An exception occurred:", error)

def line_history_top_storage_by_database(conn):

    try:
        global snowflake_conn
        global report_time
        global report_sections
        conn =snowflake_conn
                
        sql_query=sql_header+"""
            WITH MAX_DATE AS (
                SELECT MAX (USAGE_DATE) USAGE_DATE, DATABASE_NAME 
                FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
                WHERE USAGE_DATE > DATEADD(MONTH,-1,TO_TIMESTAMP("""+report_formatted_time+"""))
                GROUP BY 2
            )
            ,DATA AS (
              SELECT 
              Q.DATABASE_NAME                                              AS DATABASE_NAME,
              ROUND(AVERAGE_DATABASE_BYTES/1024/1024/1024,2)               AS AVERAGE_DATABASE_GB,
              ROUND(AVERAGE_FAILSAFE_BYTES/1024/1024/1024,2)               AS AVERAGE_FAILSAFE_GB,
              ROUND(AVERAGE_HYBRID_TABLE_STORAGE_BYTES/1024/1024/1024  ,2) AS AVERAGE_HYBRID_TABLE_STORAGE_GB
            FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY  Q
            INNER JOIN MAX_DATE
                ON (
                    MAX_DATE.DATABASE_NAME=Q.DATABASE_NAME 
                    AND Q.USAGE_DATE=MAX_DATE.USAGE_DATE
                    )
            ORDER BY AVERAGE_DATABASE_GB desc
            LIMIT 10
            )
            SELECT 
                DISTINCT 
                DATABASE_NAME
            FROM DATA;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        if int(cur.rowcount)!=0:
            for (database_name) in cur:
                
                html_file=html_header+"""
                [
                'DATE',
                'AVERAGE_DATABASE_GB',
                'AVERAGE_FAILSAFE_GB',
                'AVERAGE_HYBRID_TABLE_STORAGE_GB'
                ],
                """
        
                sql_query_details=sql_header+"""
                WITH DATA AS (
                    SELECT 
                        USAGE_DATE                                                   AS DATE,
                        ROUND(AVERAGE_DATABASE_BYTES/1024/1024/1024,2)               AS AVERAGE_DATABASE_GB,
                        ROUND(AVERAGE_FAILSAFE_BYTES/1024/1024/1024,2)               AS AVERAGE_FAILSAFE_GB,
                        ROUND(AVERAGE_HYBRID_TABLE_STORAGE_BYTES/1024/1024/1024  ,2) AS AVERAGE_HYBRID_TABLE_STORAGE_GB
                    FROM snowflake.account_usage.DATABASE_STORAGE_USAGE_HISTORY
                    WHERE  TO_DATE(USAGE_DATE) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                    AND DATABASE_NAME='"""+str(database_name[0])+"""'
                ORDER BY USAGE_DATE  
                )
                SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA;
                """
                cur_details = conn.cursor()
                cur_details.execute(sql_query_details)
                
                if int(cur_details.rowcount)!=0:
                    for (row_data) in cur_details:
                        html_file=html_file+str(row_data[0])+""","""
                    
                    html_file=html_file+  html_body1 +"""
                    row[1],row[2],row[3]
                    """+ html_body2 +"""
                    title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
                    Chart Creation Date: """+report_formatted_time+"""
                    Storage usage for database """+str(database_name[0]).lower()+"""`,"""+ html_line_hour_tail  
            
                    create_output_file('top_storage_for_database_'+  database_name[0].lower()  +'.html',html_file)
                    report_sections["B - Storage"].update({'top_storage_for_database_'+  database_name[0].lower()  +'.html':'line'})
        
    except Exception as error:
        print("[line_history_top_storage_by_database]: An exception occurred:", error)

def table_history_recent_changed_network_policies(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
              SELECT 
                ROW_NUMBER() OVER (ORDER BY LAST_ALTERED  DESC)       AS TOP_N,
                LAST_ALTERED              AS LAST_ALTERED,
                ID                        AS ID,
                NAME                      AS NAME,
                OWNER                     AS OWNER,
                OWNER_ROLE_TYPE           AS OWNER_ROLE_TYPE,
                CREATED                   AS CREATED,
                COMMENT                   AS COMMENT
              FROM SNOWFLAKE.ACCOUNT_USAGE.NETWORK_POLICIES 
              WHERE LAST_ALTERED > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                OR CREATED > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
              ORDER BY LAST_ALTERED DESC
              
        )
        SELECT 
            *
        FROM DATA
        WHERE TOP_N<=100
        ORDER BY TOP_N;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Recent changes on network policies</h3>
        <table class="tabla1">
        <tr>
        <th >TOP_N</th>
        <th >LAST_ALTERED</th>
        <th >ID</th>
        <th >NAME</th>
        <th >OWNER</th>
        <th >OWNER_ROLE_TYPE</th>
        <th >CREATED</th>
        <th >COMMENT</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_TOP_N, ROW_LAST_ALTERED, ROW_ID, ROW_NAME, ROW_OWNER, ROW_OWNER_ROLE_TYPE, ROW_CREATED, ROW_COMMENT) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_TOP_N)+"""</td> 
                <td>"""+str(ROW_LAST_ALTERED)+"""</td> 
                 <td>"""+str(ROW_ID)+"""</td> 
                 <td>"""+str(ROW_NAME)+"""</td> 
                 <td>"""+str(ROW_OWNER)+"""</td> 
                 <td>"""+str(ROW_OWNER_ROLE_TYPE)+"""</td> 
                 <td>"""+str(ROW_CREATED)+"""</td> 
                 <td>"""+str(ROW_COMMENT)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
            create_output_file('recent_changes_on_network_policies.html',html_file)
            report_sections["E - Security"].update({'recent_changes_on_network_policies.html':'table'})

    
    except Exception as error:
        print("[table_history_recent_changed_network_policies]: An exception occurred:", error)

def table_history_recent_changed_network_rules(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
              SELECT 
                ROW_NUMBER() OVER (ORDER BY LAST_ALTERED  DESC)       AS TOP_N,
                LAST_ALTERED        AS LAST_ALTERED,
                ID                  AS ID,
                NAME                AS NAME,
                SCHEMA_ID           AS SCHEMA_ID,
                SCHEMA              AS SCHEMA_NAME,
                DATABASE_ID         AS DATABASE_ID,
                DATABASE            AS DATABASE_NAME,
                OWNER               AS OWNER,
                OWNER_ROLE_TYPE     AS OWNER_ROLE_TYPE,            
                CREATED             AS CREATED,           
                DELETED             AS DELETED,
                COMMENT             AS COMMENT
              FROM SNOWFLAKE.ACCOUNT_USAGE.NETWORK_RULES    
              WHERE LAST_ALTERED > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                OR CREATED > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
              ORDER BY LAST_ALTERED DESC
        )
        SELECT 
            *
        FROM DATA
        WHERE TOP_N<=100
        ORDER BY TOP_N;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Recent changes on network rules</h3>
        <table class="tabla1">
        <tr>
        <th >TOP_N</th>
        <th >LAST_ALTERED</th>
        <th >ID</th>
        <th >NAME</th>
        <th >SCHEMA_ID</th>
        <th >SCHEMA_NAME</th>
        <th >DATABASE_ID</th>
        <th >DATABASE_NAME</th>
        <th >OWNER</th>
        <th >OWNER_ROLE_TYPE</th>
        <th >CREATED</th>
        <th >DELETED</th>
        <th >COMMENT</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_TOP_N, ROW_LAST_ALTERED, ROW_ID, ROW_NAME, ROW_SCHEMA_ID, ROW_SCHEMA_NAME, ROW_DATABASE_ID, ROW_DATABASE_NAME, ROW_OWNER, ROW_OWNER_ROLE_TYPE, ROW_CREATED, ROW_DELETED, ROW_COMMENT) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_TOP_N)+"""</td> 
                <td>"""+str(ROW_LAST_ALTERED)+"""</td> 
                 <td>"""+str(ROW_ID)+"""</td> 
                 <td>"""+str(ROW_NAME)+"""</td> 
                 <td>"""+str(ROW_SCHEMA_ID)+"""</td> 
                 <td>"""+str(ROW_SCHEMA_NAME)+"""</td> 
                 <td>"""+str(ROW_DATABASE_ID)+"""</td> 
                 <td>"""+str(ROW_DATABASE_NAME)+"""</td> 
                 <td>"""+str(ROW_OWNER)+"""</td> 
                 <td>"""+str(ROW_OWNER_ROLE_TYPE)+"""</td> 
                 <td>"""+str(ROW_CREATED)+"""</td> 
                 <td>"""+str(ROW_DELETED)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_COMMENT)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('recent_changes_on_network_rules.html',html_file)
        report_sections["E - Security"].update({'recent_changes_on_network_rules.html':'table'})

    
    except Exception as error:
        print("[table_history_recent_changed_network_rules]: An exception occurred:", error)

def table_history_recent_changed_password_policies(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
              SELECT 
                ROW_NUMBER() OVER (ORDER BY LAST_ALTERED  DESC)       AS TOP_N,
                LAST_ALTERED                    AS LAST_ALTERED,
                NAME                            AS NAME,
                ID                              AS ID,
                SCHEMA_ID                       AS SCHEMA_ID,
                SCHEMA                          AS SCHEMA,
                DATABASE_ID                     AS DATABASE_ID,
                DATABASE                        AS DATABASE,
                OWNER                           AS OWNER,
                OWNER_ROLE_TYPE                 AS OWNER_ROLE_TYPE,
                PASSWORD_MIN_LENGTH             AS PASSWORD_MIN_LENGTH,
                PASSWORD_MAX_LENGTH             AS PASSWORD_MAX_LENGTH,
                PASSWORD_MIN_UPPER_CASE_CHARS   AS PASSWORD_MIN_UPPER_CASE_CHARS,
                PASSWORD_MIN_LOWER_CASE_CHARS   AS PASSWORD_MIN_LOWER_CASE_CHARS,
                PASSWORD_MIN_NUMERIC_CHARS      AS PASSWORD_MIN_NUMERIC_CHARS,
                PASSWORD_MIN_SPECIAL_CHARS      AS PASSWORD_MIN_SPECIAL_CHARS,
                PASSWORD_MIN_AGE_DAYS           AS PASSWORD_MIN_AGE_DAYS,
                PASSWORD_MAX_AGE_DAYS           AS PASSWORD_MAX_AGE_DAYS,
                PASSWORD_MAX_RETRIES            AS PASSWORD_MAX_RETRIES,
                PASSWORD_LOCKOUT_TIME_MINS      AS PASSWORD_LOCKOUT_TIME_MINS,
                CREATED                         AS CREATED,
                DELETED                         AS DELETED,
                PASSWORD_HISTORY                AS PASSWORD_HISTORY,
                COMMENT                         AS COMMENT
              FROM SNOWFLAKE.ACCOUNT_USAGE.PASSWORD_POLICIES   
              WHERE LAST_ALTERED > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                OR CREATED > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
              ORDER BY LAST_ALTERED DESC
        )
        SELECT 
            *
        FROM DATA
        WHERE TOP_N<=100
        ORDER BY TOP_N;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Recent changes on password policies</h3>
        <table class="tabla1">
        <tr>
        <th >TOP_N</th>
        <th >LAST_ALTERED</th>
        <th >NAME</th>
        <th >ID</th>
        <th >SCHEMA_ID</th>
        <th >SCHEMA</th>
        <th >DATABASE_ID</th>
        <th >DATABASE</th>
        <th >OWNER</th>
        <th >OWNER_ROLE_TYPE</th>
        <th >PASSWORD_MIN_LENGTH</th>
        <th >PASSWORD_MAX_LENGTH</th>
        <th >PASSWORD_MIN_UPPER_CASE_CHARS</th>
        <th >PASSWORD_MIN_LOWER_CASE_CHARS</th>
        <th >PASSWORD_MIN_NUMERIC_CHARS</th>
        <th >PASSWORD_MIN_SPECIAL_CHARS</th>
        <th >PASSWORD_MIN_AGE_DAYS</th>
        <th >PASSWORD_MAX_AGE_DAYS</th>
        <th >PASSWORD_MAX_RETRIES</th>
        <th >PASSWORD_LOCKOUT_TIME_MINS</th>
        <th >CREATED</th>
        <th >DELETED</th>
        <th >PASSWORD_HISTORY</th>
        <th >COMMENT</th>
        """
    
        if int(cur.rowcount)!=0:
            for ( ROW_TOP_N, ROW_LAST_ALTERED, ROW_NAME, ROW_ID,  ROW_SCHEMA_ID, ROW_SCHEMA, ROW_DATABASE_ID, ROW_DATABASE, ROW_OWNER, ROW_OWNER_ROLE_TYPE, ROW_PASSWORD_MIN_LENGTH,  ROW_PASSWORD_MAX_LENGTH, ROW_PASSWORD_MIN_UPPER_CASE_CHARS, ROW_PASSWORD_MIN_LOWER_CASE_CHARS, ROW_PASSWORD_MIN_NUMERIC_CHARS, ROW_PASSWORD_MIN_SPECIAL_CHARS, ROW_PASSWORD_MIN_AGE_DAYS,  ROW_PASSWORD_MAX_AGE_DAYS, ROW_PASSWORD_MAX_RETRIES, ROW_PASSWORD_LOCKOUT_TIME_MINS, ROW_CREATED, ROW_DELETED, ROW_PASSWORD_HISTORY, ROW_COMMENT) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_TOP_N)+"""</td> 
                <td>"""+str(ROW_LAST_ALTERED)+"""</td> 
                 <td>"""+str(ROW_NAME)+"""</td> 
                 <td>"""+str(ROW_ID)+"""</td> 
                 <td>"""+str(ROW_SCHEMA_ID)+"""</td> 
                 <td>"""+str(ROW_SCHEMA)+"""</td> 
                 <td>"""+str(ROW_DATABASE_ID)+"""</td> 
                 <td>"""+str(ROW_DATABASE)+"""</td> 
                 <td>"""+str(ROW_OWNER)+"""</td> 
                 <td>"""+str(ROW_OWNER_ROLE_TYPE)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_MIN_LENGTH)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_MAX_LENGTH)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_MIN_UPPER_CASE_CHARS)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_MIN_LOWER_CASE_CHARS)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_MIN_NUMERIC_CHARS)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_MIN_SPECIAL_CHARS)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_MIN_AGE_DAYS)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_MAX_AGE_DAYS)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_MAX_RETRIES)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_LOCKOUT_TIME_MINS)+"""</td> 
                 <td>"""+str(ROW_CREATED)+"""</td> 
                 <td>"""+str(ROW_DELETED)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_HISTORY)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_COMMENT)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('recent_changes_on_password_policies.html',html_file)
        report_sections["E - Security"].update({'recent_changes_on_password_policies.html':'table'})

    
    except Exception as error:
        print("[table_history_recent_changed_password_policies]: An exception occurred:", error)

def table_history_recent_changed_masking_policies(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
              SELECT 
                ROW_NUMBER() OVER (ORDER BY LAST_ALTERED  DESC)       AS TOP_N,
                LAST_ALTERED        AS LAST_ALTERED,
                POLICY_NAME         AS POLICY_NAME,
                POLICY_ID           AS POLICY_ID,
                POLICY_SCHEMA_ID    AS POLICY_SCHEMA_ID,
                POLICY_SCHEMA       AS POLICY_SCHEMA,
                POLICY_CATALOG_ID   AS POLICY_CATALOG_ID,
                POLICY_CATALOG      AS POLICY_CATALOG,
                POLICY_OWNER        AS POLICY_OWNER,
                POLICY_SIGNATURE    AS POLICY_SIGNATURE,
                POLICY_RETURN_TYPE  AS POLICY_RETURN_TYPE,
                POLICY_BODY         AS POLICY_BODY,
                CREATED             AS CREATED,
                DELETED             AS DELETED,
                OWNER_ROLE_TYPE     AS OWNER_ROLE_TYPE,
                OPTIONS             AS OPTIONS,
                POLICY_COMMENT      AS POLICY_COMMENT
              FROM SNOWFLAKE.ACCOUNT_USAGE.MASKING_POLICIES   
              WHERE LAST_ALTERED > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                OR CREATED > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
              ORDER BY LAST_ALTERED DESC
        )
        SELECT 
            *
        FROM DATA
        WHERE TOP_N<=100
        ORDER BY TOP_N;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Recent changes on masking policies</h3>
        <table class="tabla1">
        <tr>
        <th > TOP_N</th>
        <th > LAST_ALTERED</th>
        <th > POLICY_NAME</th>
        <th > POLICY_ID</th>
        <th > POLICY_SCHEMA_ID</th>
        <th > POLICY_SCHEMA</th>
        <th > POLICY_CATALOG_ID</th>
        <th > POLICY_CATALOG</th>
        <th > POLICY_OWNER</th>
        <th > POLICY_SIGNATURE</th>
        <th > POLICY_RETURN_TYPE</th>
        <th > POLICY_BODY</th>
        <th > CREATED</th>
        <th > DELETED</th>
        <th > OWNER_ROLE_TYPE</th>
        <th > OPTIONS</th>
        <th > POLICY_COMMENT</th>
        """
    
        if int(cur.rowcount)!=0:
            for ( ROW_TOP_N, ROW_LAST_ALTERED, ROW_POLICY_NAME, ROW_POLICY_ID, ROW_POLICY_SCHEMA_ID, ROW_POLICY_SCHEMA, ROW_POLICY_CATALOG_ID, ROW_POLICY_CATALOG, ROW_POLICY_OWNER, ROW_POLICY_SIGNATURE, ROW_POLICY_RETURN_TYPE, ROW_POLICY_BODY, ROW_CREATED, ROW_DELETED, ROW_OWNER_ROLE_TYPE, ROW_OPTIONS, ROW_POLICY_COMMENT ) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(ROW_TOP_N)+"""</td> 
                 <td>"""+str(ROW_LAST_ALTERED)+"""</td> 
                 <td>"""+str(ROW_POLICY_NAME)+"""</td> 
                 <td>"""+str(ROW_POLICY_ID)+"""</td> 
                 <td>"""+str(ROW_POLICY_SCHEMA_ID)+"""</td> 
                 <td>"""+str(ROW_POLICY_SCHEMA)+"""</td> 
                 <td>"""+str(ROW_POLICY_CATALOG_ID)+"""</td> 
                 <td>"""+str(ROW_POLICY_CATALOG)+"""</td> 
                 <td>"""+str(ROW_POLICY_OWNER)+"""</td> 
                 <td>"""+str(ROW_POLICY_SIGNATURE)+"""</td> 
                 <td>"""+str(ROW_POLICY_RETURN_TYPE)+"""</td> 
                 <td>"""+str(ROW_POLICY_BODY)+"""</td> 
                 <td>"""+str(ROW_CREATED )+"""</td> 
                 <td>"""+str(ROW_DELETED )+"""</td> 
                 <td>"""+str(ROW_OWNER_ROLE_TYPE)+"""</td> 
                 <td>"""+str(ROW_OPTIONS )+"""</td> 
                 <td class="cell_grow">"""+str(ROW_POLICY_COMMENT )+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('recent_changes_on_masking_policies.html',html_file)
        report_sections["E - Security"].update({'recent_changes_on_masking_policies.html':'table'})

    
    except Exception as error:
        print("[table_history_recent_changed_masking_policies]: An exception occurred:", error)

def table_history_recent_changed_row_access_policies(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
              SELECT 
                ROW_NUMBER() OVER (ORDER BY LAST_ALTERED  DESC)       AS TOP_N,
                LAST_ALTERED,
                POLICY_NAME,
                POLICY_ID,
                POLICY_SCHEMA_ID,
                POLICY_SCHEMA,
                POLICY_CATALOG_ID,
                POLICY_CATALOG,
                POLICY_OWNER,
                POLICY_SIGNATURE,
                POLICY_RETURN_TYPE,
                POLICY_BODY,
                CREATED,
                DELETED,
                OWNER_ROLE_TYPE,
                OPTIONS,
                POLICY_COMMENT
              FROM SNOWFLAKE.ACCOUNT_USAGE.ROW_ACCESS_POLICIES   
              WHERE LAST_ALTERED > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                OR CREATED > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+""")) 
              ORDER BY LAST_ALTERED DESC
        )
        SELECT 
            *
        FROM DATA
        WHERE TOP_N<=100
        ORDER BY TOP_N;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Recent changes on row access policies</h3>
        <table class="tabla1">
        <tr>
        <th >TOP_N</th>
        <th >LAST_ALTERED</th>
        <th >POLICY_NAME</th>
        <th >POLICY_ID</th>
        <th >POLICY_SCHEMA_ID</th>
        <th >POLICY_SCHEMA</th>
        <th >POLICY_CATALOG_ID</th>
        <th >POLICY_CATALOG</th>
        <th >POLICY_OWNER</th>
        <th >POLICY_SIGNATURE</th>
        <th >POLICY_RETURN_TYPE</th>
        <th >POLICY_BODY</th>
        <th >CREATED</th>
        <th >DELETED</th>
        <th >OWNER_ROLE_TYPE</th>
        <th >OPTIONS</th>
        <th >POLICY_COMMENT</th>
        """
    
        if int(cur.rowcount)!=0:
            for ( ROW_TOP_N, ROW_LAST_ALTERED, ROW_POLICY_NAME, ROW_POLICY_ID, ROW_POLICY_SCHEMA_ID, ROW_POLICY_SCHEMA, ROW_POLICY_CATALOG_ID, ROW_POLICY_CATALOG, ROW_POLICY_OWNER, ROW_POLICY_SIGNATURE, ROW_POLICY_RETURN_TYPE, ROW_POLICY_BODY, ROW_CREATED, ROW_DELETED, ROW_OWNER_ROLE_TYPE, ROW_OPTIONS, ROW_POLICY_COMMENT ) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(ROW_TOP_N)+"""</td> 
                 <td>"""+str(ROW_LAST_ALTERED)+"""</td> 
                 <td>"""+str(ROW_POLICY_NAME)+"""</td> 
                 <td>"""+str(ROW_POLICY_ID)+"""</td> 
                 <td>"""+str(ROW_POLICY_SCHEMA_ID)+"""</td> 
                 <td>"""+str(ROW_POLICY_SCHEMA)+"""</td> 
                 <td>"""+str(ROW_POLICY_CATALOG_ID)+"""</td> 
                 <td>"""+str(ROW_POLICY_CATALOG)+"""</td> 
                 <td>"""+str(ROW_POLICY_OWNER)+"""</td> 
                 <td>"""+str(ROW_POLICY_SIGNATURE)+"""</td> 
                 <td>"""+str(ROW_POLICY_RETURN_TYPE)+"""</td> 
                 <td>"""+str(ROW_POLICY_BODY)+"""</td> 
                 <td>"""+str(ROW_CREATED )+"""</td> 
                 <td>"""+str(ROW_DELETED )+"""</td> 
                 <td>"""+str(ROW_OWNER_ROLE_TYPE)+"""</td> 
                 <td>"""+str(ROW_OPTIONS )+"""</td> 
                 <td>"""+str(ROW_POLICY_COMMENT )+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('recent_changes_on_row_access_policies.html',html_file)
        report_sections["E - Security"].update({'recent_changes_on_row_access_policies.html':'table'})

    
    except Exception as error:
        print("[table_history_recent_changed_row_access_policies]: An exception occurred:", error)

def table_history_users_with_recent_password_changes(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
              SELECT 
                ROW_NUMBER() OVER (ORDER BY PASSWORD_LAST_SET_TIME  DESC)       AS TOP_N,
                PASSWORD_LAST_SET_TIME      AS PASSWORD_LAST_SET_TIME,
                USER_ID                     AS USER_ID,
                NAME                        AS NAME,
                CREATED_ON                  AS CREATED_ON,
                DELETED_ON                  AS DELETED_ON,
                LOGIN_NAME                  AS LOGIN_NAME,
                DISPLAY_NAME                AS DISPLAY_NAME,
                FIRST_NAME                  AS FIRST_NAME,
                LAST_NAME                   AS LAST_NAME,
                EMAIL                       AS EMAIL,
                MUST_CHANGE_PASSWORD        AS MUST_CHANGE_PASSWORD,
                HAS_PASSWORD                AS HAS_PASSWORD,
                COMMENT                     AS COMMENT,
                DISABLED                    AS DISABLED,
                SNOWFLAKE_LOCK              AS SNOWFLAKE_LOCK,
                DEFAULT_WAREHOUSE           AS DEFAULT_WAREHOUSE,
                DEFAULT_NAMESPACE           AS DEFAULT_NAMESPACE,
                DEFAULT_ROLE                AS DEFAULT_ROLE,
                EXT_AUTHN_DUO               AS EXT_AUTHN_DUO,
                EXT_AUTHN_UID               AS EXT_AUTHN_UID,
                BYPASS_MFA_UNTIL            AS BYPASS_MFA_UNTIL,
                LAST_SUCCESS_LOGIN          AS LAST_SUCCESS_LOGIN,
                EXPIRES_AT                  AS EXPIRES_AT,
                LOCKED_UNTIL_TIME           AS LOCKED_UNTIL_TIME,
                HAS_RSA_PUBLIC_KEY          AS HAS_RSA_PUBLIC_KEY,
                OWNER                       AS OWNER,
                DEFAULT_SECONDARY_ROLE      AS DEFAULT_SECONDARY_ROLE,
              FROM SNOWFLAKE.ACCOUNT_USAGE.USERS    
              WHERE PASSWORD_LAST_SET_TIME > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
              ORDER BY PASSWORD_LAST_SET_TIME DESC
        )
        SELECT 
            *
        FROM DATA
        WHERE TOP_N<=100
        ORDER BY TOP_N;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Users with recent password changes</h3>
        <table class="tabla1">
        <tr>
        <th >TOP_N</th>
        <th >PASSWORD_LAST_SET_TIME</th>
        <th >USER_ID</th>
        <th >NAME</th>
        <th >CREATED_ON</th>
        <th >DELETED_ON</th>
        <th >LOGIN_NAME</th>
        <th >DISPLAY_NAME</th>
        <th >FIRST_NAME</th>
        <th >LAST_NAME</th>
        <th >EMAIL</th>
        <th >MUST_CHANGE_PASSWORD</th>
        <th >HAS_PASSWORD</th>
        <th >COMMENT</th>
        <th >DISABLED</th>
        <th >SNOWFLAKE_LOCK</th>
        <th >DEFAULT_WAREHOUSE</th>
        <th >DEFAULT_NAMESPACE</th>
        <th >DEFAULT_ROLE</th>
        <th >EXT_AUTHN_DUO</th>
        <th >EXT_AUTHN_UID</th>
        <th >BYPASS_MFA_UNTIL</th>
        <th >LAST_SUCCESS_LOGIN</th>
        <th >EXPIRES_AT</th>
        <th >LOCKED_UNTIL_TIME</th>
        <th >HAS_RSA_PUBLIC_KEY</th>
        <th >OWNER</th>
        <th >DEFAULT_SECONDARY_ROLE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_TOP_N, ROW_PASSWORD_LAST_SET_TIME, ROW_USER_ID, ROW_NAME, ROW_CREATED_ON, ROW_DELETED_ON, ROW_LOGIN_NAME, ROW_DISPLAY_NAME, ROW_FIRST_NAME, ROW_LAST_NAME, ROW_EMAIL, ROW_MUST_CHANGE_PASSWORD, ROW_HAS_PASSWORD, ROW_COMMENT, ROW_DISABLED, ROW_SNOWFLAKE_LOCK, ROW_DEFAULT_WAREHOUSE, ROW_DEFAULT_NAMESPACE, ROW_DEFAULT_ROLE, ROW_EXT_AUTHN_DUO, ROW_EXT_AUTHN_UID, ROW_BYPASS_MFA_UNTIL,  ROW_LAST_SUCCESS_LOGIN,  ROW_EXPIRES_AT, ROW_LOCKED_UNTIL_TIME, ROW_HAS_RSA_PUBLIC_KEY, ROW_OWNER,  ROW_DEFAULT_SECONDARY_ROLE) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(ROW_TOP_N)+"""</td> 
                 <td>"""+str(ROW_PASSWORD_LAST_SET_TIME)+"""</td> 
                 <td>"""+str(ROW_USER_ID)+"""</td> 
                 <td>"""+str(ROW_NAME)+"""</td> 
                 <td>"""+str(ROW_CREATED_ON)+"""</td> 
                 <td>"""+str(ROW_DELETED_ON)+"""</td> 
                 <td>"""+str(ROW_LOGIN_NAME)+"""</td> 
                 <td>"""+str(ROW_DISPLAY_NAME)+"""</td> 
                 <td>"""+str(ROW_FIRST_NAME)+"""</td> 
                 <td>"""+str(ROW_LAST_NAME)+"""</td> 
                 <td>"""+str(ROW_EMAIL)+"""</td> 
                 <td>"""+str(ROW_MUST_CHANGE_PASSWORD)+"""</td> 
                 <td>"""+str(ROW_HAS_PASSWORD)+"""</td> 
                 <td>"""+str(ROW_COMMENT)+"""</td> 
                 <td>"""+str(ROW_DISABLED)+"""</td> 
                 <td>"""+str(ROW_SNOWFLAKE_LOCK)+"""</td> 
                 <td>"""+str(ROW_DEFAULT_WAREHOUSE)+"""</td> 
                 <td>"""+str(ROW_DEFAULT_NAMESPACE)+"""</td> 
                 <td>"""+str(ROW_DEFAULT_ROLE)+"""</td> 
                 <td>"""+str(ROW_EXT_AUTHN_DUO)+"""</td> 
                 <td>"""+str(ROW_EXT_AUTHN_UID)+"""</td> 
                 <td>"""+str(ROW_BYPASS_MFA_UNTIL )+"""</td> 
                 <td>"""+str(ROW_LAST_SUCCESS_LOGIN )+"""</td> 
                 <td>"""+str(ROW_EXPIRES_AT )+"""</td> 
                 <td>"""+str(ROW_LOCKED_UNTIL_TIME )+"""</td> 
                 <td>"""+str(ROW_HAS_RSA_PUBLIC_KEY )+"""</td> 
                 <td>"""+str(ROW_OWNER )+"""</td> 
                 <td class="cell_grow">"""+str(ROW_DEFAULT_SECONDARY_ROLE )+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('users_with_recent_password_changes.html',html_file)
        report_sections["E - Security"].update({'users_with_recent_password_changes.html':'table'})

    
    except Exception as error:
        print("[table_history_users_with_recent_password_changes]: An exception occurred:", error)

def bar_month_sessions_by_authentication_method(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
            
        html_file=html_header
                
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('MONTH',CREATED_ON::TIMESTAMP_NTZ)    AS DATE 
                ,AUTHENTICATION_METHOD                              AS AUTHENTICATION_METHOD
                ,count(*)                                           AS SESSION_COUNT
            FROM snowflake.account_usage.SESSIONS                      
            WHERE CREATED_ON > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+""")) 
            GROUP BY 1,2
            ORDER BY 1
        )
        SELECT *
        FROM DATA
            PIVOT(  sum (SESSION_COUNT) FOR AUTHENTICATION_METHOD IN (ANY ORDER BY AUTHENTICATION_METHOD ) DEFAULT ON NULL (0) ) 
        ORDER BY DATE
        """
        cur= conn.cursor()
        cur.execute(sql_query)
        
        column_count=len(cur.description)
        headers =[i[0] for i in cur.description]  
        html_file=html_file+str(headers).replace('"','')+", \n"      
            
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('MONTH',CREATED_ON::TIMESTAMP_NTZ)    AS DATE 
                ,AUTHENTICATION_METHOD                              AS AUTHENTICATION_METHOD
                ,count(*)                                           AS SESSION_COUNT
            FROM snowflake.account_usage.SESSIONS                      
            WHERE CREATED_ON > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+""")) 
            GROUP BY 1,2
            ORDER BY 1
        )
        , DATA_PIVOT AS (
        SELECT *
        FROM DATA
            PIVOT(  sum (SESSION_COUNT) FOR AUTHENTICATION_METHOD IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) ) 
        ORDER BY DATE
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA  FROM DATA_PIVOT;
        """
        cur = conn.cursor()
        cur.execute(sql_query)
    
        if int(cur.rowcount)!=0:
            counter=0
            for (row_data) in cur:
                counter=counter+1
                if counter==cur.rowcount:
                    html_file=html_file+str(row_data[0])
                else:
                    html_file=html_file+str(row_data[0])+""","""

            html_file=html_file+html_body1
            
            for i in range(1, column_count):
                if i==column_count-1:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""

            html_file=html_file+html_body2+"""
            title:  `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Sessions by authentication method per month`,"""+html_bar_month_tail
                
            create_output_file('sessions_by_authentication_method_per_month.html',html_file)
            report_sections["E - Security"].update({'sessions_by_authentication_method_per_month.html':'bar'})
            
    except Exception as error:
        print("[bar_month_sessions_by_authentication_method]: An exception occurred:", error)
 
def bar_week_sessions_by_authentication_method(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
            
        html_file=html_header
                
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('DAY',CREATED_ON::TIMESTAMP_NTZ)  AS DATE 
                ,AUTHENTICATION_METHOD                          AS AUTHENTICATION_METHOD
                ,count(*)                                       AS SESSION_COUNT
            FROM snowflake.account_usage.SESSIONS                      
            WHERE CREATED_ON > DATEADD(DAY,-7,TO_TIMESTAMP("""+report_formatted_time+""")) 
            GROUP BY 1,2
            ORDER BY 1
        )
        SELECT *
        FROM DATA
            PIVOT(  sum (SESSION_COUNT) FOR AUTHENTICATION_METHOD IN ( ANY ORDER BY AUTHENTICATION_METHOD) DEFAULT ON NULL (0) ) 
        ORDER BY DATE
        LIMIT 1
        """
        cur= conn.cursor()
        cur.execute(sql_query)
        
        column_count=len(cur.description)
        headers =[i[0] for i in cur.description]  
        html_file=html_file+str(headers).replace('"','')+", \n"                    
    
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('DAY',CREATED_ON::TIMESTAMP_NTZ)  AS DATE 
                ,AUTHENTICATION_METHOD                          AS AUTHENTICATION_METHOD
                ,count(*)                                       AS SESSION_COUNT
            FROM snowflake.account_usage.SESSIONS                      
            WHERE CREATED_ON > DATEADD(DAY,-7,TO_TIMESTAMP("""+report_formatted_time+""")) 
            GROUP BY 1,2
            ORDER BY 1
        )
        , DATA_PIVOT AS (
        SELECT *
        FROM DATA
            PIVOT(  sum (SESSION_COUNT) FOR AUTHENTICATION_METHOD IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) ) 
        ORDER BY DATE
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA  FROM DATA_PIVOT;
        """
        cur = conn.cursor()
        cur.execute(sql_query)
    
        if int(cur.rowcount)!=0:
            counter=0
            for (row_data) in cur:
                counter=counter+1
                if counter==cur.rowcount:
                    html_file=html_file+str(row_data[0])
                else:
                    html_file=html_file+str(row_data[0])+""","""

            html_file=html_file+html_body1
            
            for i in range(1, column_count):
                if i==column_count-1:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""

            html_file=html_file+html_body2+"""
            title:  `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Sessions by authentication method for the last week`,"""+html_bar_day_tail
                
            create_output_file('sessions_by_authentication_method_for_last_week.html',html_file)
            report_sections["E - Security"].update({'sessions_by_authentication_method_for_last_week.html':'bar'})
            
    except Exception as error:
        print("[bar_week_sessions_by_authentication_method]: An exception occurred:", error)
          
def line_history_sessions_by_authentication_method(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('HOUR',CREATED_ON::TIMESTAMP_NTZ)     AS DATE 
                ,AUTHENTICATION_METHOD                              AS AUTHENTICATION_METHOD
                ,count(*)                                           AS SESSION_COUNT
            FROM snowflake.account_usage.SESSIONS                      
            WHERE CREATED_ON > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+""")) 
            GROUP BY 1,2
            ORDER BY 1
        )
        SELECT *
        FROM DATA
            PIVOT(  sum (SESSION_COUNT) FOR AUTHENTICATION_METHOD IN ( ANY ORDER BY AUTHENTICATION_METHOD) DEFAULT ON NULL (0) ) 
        ORDER BY DATE
        """
        cur= conn.cursor()
        cur.execute(sql_query)
        
        column_count=len(cur.description)
        headers =[i[0] for i in cur.description]    
        html_file=html_header+str(headers).replace('"','')+", \n"   
        
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('HOUR',CREATED_ON::TIMESTAMP_NTZ)     AS DATE 
                ,AUTHENTICATION_METHOD                              AS AUTHENTICATION_METHOD
                ,count(*)                                           AS SESSION_COUNT
            FROM snowflake.account_usage.SESSIONS                      
            WHERE CREATED_ON > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+""")) 
            GROUP BY 1,2
            ORDER BY 1
        )
        , DATA_PIVOT AS (
        SELECT *
        FROM DATA
            PIVOT(  sum (SESSION_COUNT) FOR AUTHENTICATION_METHOD IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) ) 
        ORDER BY DATE
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA  FROM DATA_PIVOT;
        """
        cur = conn.cursor()
        cur.execute(sql_query)
    
        if int(cur.rowcount)!=0:
            counter=0
            for (row_data) in cur:
                counter=counter+1
                if counter==cur.rowcount:
                    html_file=html_file+str(row_data[0])
                else:
                    html_file=html_file+str(row_data[0])+""","""

            html_file=html_file+html_body1   

            for i in range(1, column_count):
                if i==column_count-1:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""

            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Sessions by authentication method`,
            """+html_line_hour_tail
            
            create_output_file('sessions_by_authentication_method.html',html_file)
            report_sections["E - Security"].update({'sessions_by_authentication_method.html':'line'})
            
    except Exception as error:
        print("[line_history_sessions_by_authentication_method]: An exception occurred:", error)

def table_history_users_without_sessions_last_6_months(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH HIST AS (
              SELECT 
                USER_NAME , MAX(CREATED_ON) AS LAST_SESSION_DATE
              FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS     
              GROUP BY 1 
        ), LAST_6_MONTHS AS (
              SELECT 
                DISTINCT USER_NAME
              FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS      
              WHERE CREATED_ON > DATEADD(MONTH,-6,TO_TIMESTAMP("""+report_formatted_time+"""))
        )
        SELECT 
            *
        FROM HIST
        WHERE USER_NAME NOT IN (SELECT USER_NAME FROM LAST_6_MONTHS) AND (SELECT COUNT(*) FROM HIST)>0
        ORDER BY LAST_SESSION_DATE;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Users without sessions in last 6 months</h3>
        <table class="tabla2">
        <tr>
        <th >USER_NAME</th>
        <th >LAST_SESSION_DATE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_USER_NAME, ROW_LAST_SESSION_DATE) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(ROW_USER_NAME)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_LAST_SESSION_DATE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('users_without_sessions_in_last_6_months.html',html_file)
        report_sections["G - Maintenance"].update({'users_without_sessions_in_last_6_months.html':'table'})

    
    except Exception as error:
        print("[table_history_users_without_sessions_last_6_months]: An exception occurred:", error)

def table_history_users_without_sessions_last_3_months(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH HIST AS (
              SELECT 
                USER_NAME , MAX(CREATED_ON) AS LAST_SESSION_DATE
              FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS   
              GROUP BY 1   
        ), LAST_6_MONTHS AS (
              SELECT 
                DISTINCT USER_NAME
              FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS      
              WHERE CREATED_ON > DATEADD(MONTH,-3,TO_TIMESTAMP("""+report_formatted_time+"""))
        )
        SELECT 
            *
        FROM HIST
        WHERE USER_NAME NOT IN (SELECT USER_NAME FROM LAST_6_MONTHS) AND (SELECT COUNT(*) FROM HIST)>0
        ORDER BY LAST_SESSION_DATE;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Users without sessions in last 3 months</h3>
        <table class="tabla2">
        <tr>
        <th >USER_NAME</th>
        <th >LAST_SESSION_DATE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_USER_NAME, ROW_LAST_SESSION_DATE) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(ROW_USER_NAME)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_LAST_SESSION_DATE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('users_without_sessions_in_last_3_months.html',html_file)
        report_sections["G - Maintenance"].update({'users_without_sessions_in_last_3_months.html':'table'})

    
    except Exception as error:
        print("[table_history_users_without_sessions_last_6_months]: An exception occurred:", error)

def table_history_need_attention_tasks(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH HIST AS (
              SELECT 
                ROW_NUMBER() OVER (PARTITION  BY DATABASE_NAME, SCHEMA_NAME, NAME ORDER BY SCHEDULED_TIME  DESC)       AS TOP_N,
                NAME,
                CONDITION_TEXT,
                SCHEMA_NAME,
                TASK_SCHEMA_ID,
                DATABASE_NAME,
                TASK_DATABASE_ID,
                SCHEDULED_TIME,
                COMPLETED_TIME,
                STATE,
                RETURN_VALUE,
                QUERY_ID,
                QUERY_START_TIME,
                ERROR_CODE,
                ERROR_MESSAGE,
                GRAPH_VERSION,
                RUN_ID,
                ROOT_TASK_ID,
                SCHEDULED_FROM,
                ATTEMPT_NUMBER,
                INSTANCE_ID,
                CONFIG,
                QUERY_HASH,
                QUERY_HASH_VERSION,
                QUERY_PARAMETERIZED_HASH,
                QUERY_PARAMETERIZED_HASH_VERSION,
                GRAPH_RUN_GROUP_ID,     
                QUERY_TEXT 
              FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY       
              WHERE SCHEDULED_TIME > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+""")) 
        )
        , DATA AS (
              SELECT 
                *
              FROM HIST     
              WHERE TOP_N<=5
        )
        , WARNING_TASKS AS (
            SELECT 
                DATABASE_NAME, SCHEMA_NAME, NAME
            FROM DATA
            WHERE    STATE!='SUCCEEDED'
        )
        SELECT * 
        FROM DATA 
        WHERE (DATABASE_NAME, SCHEMA_NAME, NAME) IN (SELECT DATABASE_NAME, SCHEMA_NAME, NAME FROM WARNING_TASKS)
        ORDER BY DATABASE_NAME, SCHEMA_NAME, NAME, TOP_N
        ;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Tasks that need attention</h3>
        <table class="tabla2">
        <tr>
        <th >TOP_N</th>
        <th >TASK_NAME</th>
        <th >CONDITION_TEXT</th>
        <th >SCHEMA_NAME</th>
        <th >TASK_SCHEMA_ID</th>
        <th >DATABASE_NAME</th>
        <th >TASK_DATABASE_ID</th>
        <th >SCHEDULED_TIME</th>
        <th >COMPLETED_TIME</th>
        <th >STATE</th>
        <th >RETURN_VALUE</th>
        <th >QUERY_ID</th>
        <th >QUERY_START_TIME</th>
        <th >ERROR_CODE</th>
        <th >ERROR_MESSAGE</th>
        <th >GRAPH_VERSION</th>
        <th >RUN_ID</th>
        <th >ROOT_TASK_ID</th>
        <th >SCHEDULED_FROM</th>
        <th >ATTEMPT_NUMBER</th>
        <th >INSTANCE_ID</th>
        <th >CONFIG</th>
        <th >QUERY_HASH</th>
        <th >QUERY_HASH_VERSION</th>
        <th >QUERY_PARAMETERIZED_HASH</th>
        <th >QUERY_PARAMETERIZED_HASH_VERSION</th>
        <th >GRAPH_RUN_GROUP_ID</th>
        <th >QUERY_TEXT</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_TOP_N, ROW_NAME, ROW_CONDITION_TEXT, ROW_SCHEMA_NAME, ROW_TASK_SCHEMA_ID, ROW_DATABASE_NAME, ROW_TASK_DATABASE_ID, ROW_SCHEDULED_TIME, ROW_COMPLETED_TIME, ROW_STATE, ROW_RETURN_VALUE, ROW_QUERY_ID, ROW_QUERY_START_TIME, ROW_ERROR_CODE, ROW_ERROR_MESSAGE, ROW_GRAPH_VERSION, ROW_RUN_ID,  ROW_ROOT_TASK_ID, ROW_SCHEDULED_FROM, ROW_ATTEMPT_NUMBER, ROW_INSTANCE_ID, ROW_CONFIG, ROW_QUERY_HASH, ROW_QUERY_HASH_VERSION, ROW_QUERY_PARAMETERIZED_HASH, ROW_QUERY_PARAMETERIZED_HASH_VERSION,  ROW_GRAPH_RUN_GROUP_ID,  ROW_QUERY_TEXT) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(ROW_TOP_N)+"""</td> 
                 <td>"""+str(ROW_NAME)+"""</td> 
                 <td>"""+str(ROW_CONDITION_TEXT)+"""</td> 
                 <td>"""+str(ROW_SCHEMA_NAME)+"""</td> 
                 <td>"""+str(ROW_TASK_SCHEMA_ID)+"""</td> 
                 <td>"""+str(ROW_DATABASE_NAME)+"""</td> 
                 <td>"""+str(ROW_TASK_DATABASE_ID)+"""</td> 
                 <td>"""+str(ROW_SCHEDULED_TIME)+"""</td> 
                 <td>"""+str(ROW_COMPLETED_TIME)+"""</td> 
                 <td>"""+str(ROW_STATE)+"""</td> 
                 <td>"""+str(ROW_RETURN_VALUE)+"""</td> 
                 <td>"""+str(ROW_QUERY_ID)+"""</td> 
                 <td>"""+str(ROW_QUERY_START_TIME)+"""</td> 
                 <td>"""+str(ROW_ERROR_CODE)+"""</td> 
                 <td>"""+str(ROW_ERROR_MESSAGE)+"""</td> 
                 <td>"""+str(ROW_GRAPH_VERSION)+"""</td> 
                 <td>"""+str(ROW_RUN_ID)+"""</td> 
                 <td>"""+str(ROW_ROOT_TASK_ID)+"""</td> 
                 <td>"""+str(ROW_SCHEDULED_FROM)+"""</td> 
                 <td>"""+str(ROW_ATTEMPT_NUMBER)+"""</td> 
                 <td>"""+str(ROW_INSTANCE_ID)+"""</td> 
                 <td>"""+str(ROW_CONFIG)+"""</td> 
                 <td>"""+str(ROW_QUERY_HASH)+"""</td> 
                 <td>"""+str(ROW_QUERY_HASH_VERSION)+"""</td> 
                 <td>"""+str(ROW_QUERY_PARAMETERIZED_HASH)+"""</td> 
                 <td>"""+str(ROW_QUERY_PARAMETERIZED_HASH_VERSION)+"""</td> 
                 <td>"""+str(ROW_GRAPH_RUN_GROUP_ID)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_QUERY_TEXT)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('need_attention_tasks.html',html_file)
        report_sections["G - Maintenance"].update({'need_attention_tasks.html':'table'})

    
    except Exception as error:
        print("[table_history_need_attention_tasks]: An exception occurred:", error)

def table_history_need_attention_snowpipes(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH HIST AS (
              SELECT 
                ROW_NUMBER() OVER (PARTITION BY PIPE_CATALOG_NAME, PIPE_SCHEMA_NAME, PIPE_NAME ORDER BY LAST_LOAD_TIME  DESC)       AS TOP_N,
                FILE_NAME,
                STAGE_LOCATION,
                LAST_LOAD_TIME,
                ROW_COUNT,
                ROW_PARSED,
                FILE_SIZE,
                FIRST_ERROR_MESSAGE,
                FIRST_ERROR_LINE_NUMBER,
                FIRST_ERROR_CHARACTER_POS,
                FIRST_ERROR_COLUMN_NAME,
                ERROR_COUNT,
                ERROR_LIMIT,
                STATUS,
                TABLE_ID,
                TABLE_NAME,
                TABLE_SCHEMA_ID,
                TABLE_SCHEMA_NAME,
                TABLE_CATALOG_ID,
                TABLE_CATALOG_NAME,
                PIPE_CATALOG_NAME,
                PIPE_SCHEMA_NAME,
                PIPE_NAME,
                PIPE_RECEIVED_TIME,
                FIRST_COMMIT_TIME
              FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY        
              WHERE LAST_LOAD_TIME > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+""")) 
              AND PIPE_NAME IS NOT NULL
        )
        , DATA AS (
              SELECT 
                *
              FROM HIST     
              WHERE TOP_N<=5
        )
        , WARNING_SNOWPIPES AS (
            SELECT 
                PIPE_CATALOG_NAME, PIPE_SCHEMA_NAME, PIPE_NAME 
            FROM DATA
            WHERE    STATUS!='Loaded'
        )
        SELECT * 
        FROM DATA 
        WHERE (PIPE_CATALOG_NAME, PIPE_SCHEMA_NAME, PIPE_NAME ) IN (SELECT PIPE_CATALOG_NAME, PIPE_SCHEMA_NAME, PIPE_NAME  FROM WARNING_SNOWPIPES)
        ORDER BY PIPE_CATALOG_NAME, PIPE_SCHEMA_NAME, PIPE_NAME, TOP_N
        ;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Snowpipes that need attention</h3>
        <table class="tabla2">
        <tr>
        <th >TOP_N</th>
        <th >FILE_NAME</th>
        <th >STAGE_LOCATION</th>
        <th >LAST_LOAD_TIME</th>
        <th >ROW_COUNT</th>
        <th >ROW_PARSED</th>
        <th >FILE_SIZE</th>
        <th >FIRST_ERROR_MESSAGE</th>
        <th >FIRST_ERROR_LINE_NUMBER</th>
        <th >FIRST_ERROR_CHARACTER_POS</th>
        <th >FIRST_ERROR_COLUMN_NAME</th>
        <th >ERROR_COUNT</th>
        <th >ERROR_LIMIT</th>
        <th >STATUS</th>
        <th >TABLE_ID</th>
        <th >TABLE_NAME</th>
        <th >TABLE_SCHEMA_ID</th>
        <th >TABLE_SCHEMA_NAME</th>
        <th >TABLE_CATALOG_ID</th>
        <th >TABLE_CATALOG_NAME</th>
        <th >PIPE_CATALOG_NAME</th>
        <th >PIPE_SCHEMA_NAME</th>
        <th >PIPE_NAME</th>
        <th >PIPE_RECEIVED_TIME</th>
        <th >FIRST_COMMIT_TIME</th>
        """
    
        if int(cur.rowcount)!=0:
            for ( TOP_N, FILE_NAME, STAGE_LOCATION, LAST_LOAD_TIME, ROW_COUNT, ROW_PARSED, FILE_SIZE ,FIRST_ERROR_MESSAGE,FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_CHARACTER_POS, FIRST_ERROR_COLUMN_NAME, ERROR_COUNT, ERROR_LIMIT, STATUS, TABLE_ID, TABLE_NAME, TABLE_SCHEMA_ID, TABLE_SCHEMA_NAME, TABLE_CATALOG_ID, TABLE_CATALOG_NAME,  PIPE_CATALOG_NAME, PIPE_SCHEMA_NAME, PIPE_NAME, PIPE_RECEIVED_TIME, FIRST_COMMIT_TIME) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(TOP_N)+"""</td> 
                 <td>"""+str(FILE_NAME)+"""</td> 
                 <td>"""+str(STAGE_LOCATION)+"""</td> 
                 <td>"""+str(LAST_LOAD_TIME)+"""</td> 
                 <td>"""+str(ROW_COUNT)+"""</td> 
                 <td>"""+str(ROW_PARSED)+"""</td> 
                 <td>"""+str(FILE_SIZE)+"""</td> 
                 <td>"""+str(FIRST_ERROR_MESSAGE)+"""</td> 
                 <td>"""+str(FIRST_ERROR_LINE_NUMBER)+"""</td> 
                 <td>"""+str(FIRST_ERROR_CHARACTER_POS)+"""</td> 
                 <td>"""+str(FIRST_ERROR_COLUMN_NAME)+"""</td> 
                 <td>"""+str(ERROR_COUNT)+"""</td> 
                 <td>"""+str(ERROR_LIMIT)+"""</td> 
                 <td>"""+str(STATUS)+"""</td> 
                 <td>"""+str(TABLE_ID)+"""</td> 
                 <td>"""+str(TABLE_NAME)+"""</td> 
                 <td>"""+str(TABLE_SCHEMA_ID)+"""</td> 
                 <td>"""+str(TABLE_SCHEMA_NAME)+"""</td> 
                 <td>"""+str(TABLE_CATALOG_ID)+"""</td> 
                 <td>"""+str(TABLE_CATALOG_NAME)+"""</td> 
                 <td>"""+str(PIPE_CATALOG_NAME)+"""</td> 
                 <td>"""+str(PIPE_SCHEMA_NAME)+"""</td> 
                 <td>"""+str(PIPE_NAME)+"""</td> 
                 <td>"""+str(PIPE_RECEIVED_TIME)+"""</td> 
                 <td class="cell_grow">"""+str(FIRST_COMMIT_TIME)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('need_attention_snowpipes.html',html_file)
        report_sections["G - Maintenance"].update({'need_attention_snowpipes.html':'table'})

    except Exception as error:
        print("[table_history_need_attention_snowpipes]: An exception occurred:", error)

def table_account_non_default_parameters(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        SHOW PARAMETERS IN ACCOUNT;
        """        

        html_file=html_file+"""
        <h3>non default parameters for the account</h3>
        <table class="tabla2">
        <tr>
        <th >PARAMETER_NAME</th>
        <th >CURRENT_VALUE</th>
        <th >DEFAULT_VALUE</th>
        <th >LEVEL</th>
        <th >DESCRIPTION</th>
        """

        cur = conn.cursor()
        cur.execute(sql_query)
    
        if int(cur.rowcount)!=0:
            for ( PARAMETER_NAME, CURRENT_VALUE, DEFAULT_VALUE, LEVEL, DESCRIPTION,TYPE) in cur:
                if str(CURRENT_VALUE)!=str(DEFAULT_VALUE):
                    html_file=html_file+""" <tr> 
                    <td>"""+str(PARAMETER_NAME)+"""</td> 
                    <td>"""+str(CURRENT_VALUE)+"""</td> 
                    <td>"""+str(DEFAULT_VALUE)+"""</td> 
                    <td>"""+str(LEVEL)+"""</td> 
                    <td class="cell_grow">"""+str(DESCRIPTION)+"""</td> 
                    </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('account_non_default_parameters.html',html_file)
        report_sections["G - Maintenance"].update({'account_non_default_parameters.html':'table'})

    except Exception as error:
        #print("[table_non_default_parameters]: An exception occurred:", error)
        return

def table_warehouse_non_default_parameters(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header

        sql_query=sql_header+"""
        SELECT DISTINCT WAREHOUSE_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY 
        """
        cur = conn.cursor()
        cur.execute(sql_query)

        html_file=html_file+"""
        <h3>non default parameters for warehouses</h3>
        <table class="tabla2">
        <tr>
        <th >WAREHOUSE_NAME</th>
        <th >PARAMETER_NAME</th>
        <th >CURRENT_VALUE</th>
        <th >DEFAULT_VALUE</th>
        <th >LEVEL</th>
        <th >DESCRIPTION</th>
        """

        if int(cur.rowcount)!=0:
            for ( ROW_WAREHOUSE_NAME) in cur:              
                sql_query_details=sql_header+"""
                SHOW PARAMETERS IN WAREHOUSE """+str(ROW_WAREHOUSE_NAME[0])+""";
                """
                
                cur_details = conn.cursor()
                cur_details.execute(sql_query_details)
            
                if int(cur_details.rowcount)!=0:
                    for ( PARAMETER_NAME, CURRENT_VALUE, DEFAULT_VALUE, LEVEL, DESCRIPTION,TYPE) in cur_details:
                        if str(CURRENT_VALUE)!=str(DEFAULT_VALUE):
                            html_file=html_file+""" <tr> 
                            <td>"""+str(ROW_WAREHOUSE_NAME[0])+"""</td> 
                            <td>"""+str(PARAMETER_NAME)+"""</td> 
                            <td>"""+str(CURRENT_VALUE)+"""</td> 
                            <td>"""+str(DEFAULT_VALUE)+"""</td> 
                            <td>"""+str(LEVEL)+"""</td> 
                            <td class="cell_grow">"""+str(DESCRIPTION)+"""</td> 
                            </tr> """
            
        html_file=html_file+html_table_tail
    
        create_output_file('warehouse_non_default_parameters.html',html_file)
        report_sections["G - Maintenance"].update({'warehouse_non_default_parameters.html':'table'})

    except Exception as error:
        #print("[table_warehouse_non_default_parameters]: An exception occurred:", error)
        return

def table_database_non_default_parameters(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header

        sql_query=sql_header+"""
        SELECT DISTINCT DATABASE_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES 
        WHERE DELETED IS NULL 
        """
        cur = conn.cursor()
        cur.execute(sql_query)

        html_file=html_file+"""
        <h3>non default parameters for databases</h3>
        <table class="tabla2">
        <tr>
        <th >DATABASE_NAME</th>
        <th >PARAMETER_NAME</th>
        <th >CURRENT_VALUE</th>
        <th >DEFAULT_VALUE</th>
        <th >LEVEL</th>
        <th >DESCRIPTION</th>
        """

        if int(cur.rowcount)!=0:
            for ( ROW_DATABASE) in cur:              
                sql_query_details=sql_header+"""
                SHOW PARAMETERS IN DATABASE """+str(ROW_DATABASE[0])+""";
                """
                
                cur_details = conn.cursor()
                cur_details.execute(sql_query_details)
            
                if int(cur_details.rowcount)!=0:
                    for ( PARAMETER_NAME, CURRENT_VALUE, DEFAULT_VALUE, LEVEL, DESCRIPTION,TYPE) in cur_details:
                        if str(CURRENT_VALUE)!=str(DEFAULT_VALUE):
                            html_file=html_file+""" <tr> 
                            <td>"""+str(ROW_DATABASE[0])+"""</td> 
                            <td>"""+str(PARAMETER_NAME)+"""</td> 
                            <td>"""+str(CURRENT_VALUE)+"""</td> 
                            <td>"""+str(DEFAULT_VALUE)+"""</td> 
                            <td>"""+str(LEVEL)+"""</td> 
                            <td class="cell_grow">"""+str(DESCRIPTION)+"""</td> 
                            </tr> """
            
        html_file=html_file+html_table_tail
    
        create_output_file('database_non_default_parameters.html',html_file)
        report_sections["G - Maintenance"].update({'database_non_default_parameters.html':'table'})

    except Exception as error:
        #print("[table_database_non_default_parameters]: An exception occurred:", error)
        return

def table_warehouse_without_activity_in_last_3_months(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
           SELECT 
                WAREHOUSE_NAME, 
                MAX(END_TIME) AS LAST_USED_DATE 
           FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY  
           GROUP BY 1 
        )
        SELECT * FROM DATA
        WHERE LAST_USED_DATE  < DATEADD(MONTH,-3,TO_TIMESTAMP("""+report_formatted_time+""")) 
        ORDER BY LAST_USED_DATE DESC
        ;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Warehouses without activity in last 3_months</h3>
        <table class="tabla2">
        <tr>
        <th >WAREHOUSE_NAME</th>
        <th >LAST_USED_DATE</th>
        """
    
        if int(cur.rowcount)!=0:
            for ( WAREHOUSE_NAME, LAST_USED_DATE) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(WAREHOUSE_NAME)+"""</td> 
                 <td class="cell_grow">"""+str(LAST_USED_DATE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('warehouse_without_activity_in_last_3_months.html',html_file)
        report_sections["G - Maintenance"].update({'warehouse_without_activity_in_last_3_months.html':'table'})

    except Exception as error:
        print("[table_warehouse_without_activity_in_last_3_months]: An exception occurred:", error)

def table_warehouse_without_activity_in_last_month(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
           SELECT 
                WAREHOUSE_NAME, 
                MAX(END_TIME) AS LAST_USED_DATE 
           FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY  
           GROUP BY 1 
        )
        SELECT * FROM DATA
        WHERE LAST_USED_DATE  < DATEADD(MONTH,-1,TO_TIMESTAMP("""+report_formatted_time+""")) 
        ORDER BY LAST_USED_DATE DESC
        ;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Warehouses without activity in last month</h3>
        <table class="tabla2">
        <tr>
        <th >WAREHOUSE_NAME</th>
        <th >LAST_USED_DATE</th>
        """
    
        if int(cur.rowcount)!=0:
            for ( WAREHOUSE_NAME, LAST_USED_DATE) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(WAREHOUSE_NAME)+"""</td> 
                 <td class="cell_grow">"""+str(LAST_USED_DATE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('warehouse_without_activity_in_last_month.html',html_file)
        report_sections["G - Maintenance"].update({'warehouse_without_activity_in_last_month.html':'table'})

    except Exception as error:
        print("[table_warehouse_without_activity_in_last_month]: An exception occurred:", error)

def table_month_top_dbt_models(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (                
               SELECT 
                ROW_NUMBER() over (order by total_elapsed_time  DESC)  AS TOP_N,
                START_TIME        AS DATE, 
                query_parameterized_hash, 
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'dbt_cloud_run_id')     
                ELSE NULL  END AS dbt_cloud_run_id,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'dbt_version')     
                ELSE NULL  END AS dbt_version,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'project_name')     
                ELSE NULL  END AS dbt_project_name,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'dbt_cloud_project_id')     
                ELSE NULL  END AS dbt_cloud_project_id,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'target_name')     
                ELSE NULL  END AS dbt_target_name,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'target_database')     
                ELSE NULL  END AS dbt_target_database,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'target_schema')     
                ELSE NULL  END AS dbt_target_schema,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'node_name')     
                ELSE NULL  END AS dbt_node_name,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'node_id')     
                ELSE NULL  END AS dbt_node_id,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'materialized')     
                ELSE NULL  END AS dbt_materialized,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE(REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'node_original_file_path')  
                ELSE NULL  END AS dbt_node_original_file_path,
                ROUND(total_elapsed_time/1000,2)                AS total_elapsed_time_seg,
                query_id,
                database_id,
                database_name,
                schema_id,
                schema_name,
                query_type,
                session_id,
                user_name,
                role_name,
                warehouse_id,
                warehouse_name,
                warehouse_size,
                warehouse_type,
                cluster_number,
                query_tag,
                execution_status,
                error_code,
                error_message,
                bytes_scanned,
                percentage_scanned_from_cache,
                bytes_written,
                bytes_written_to_result,
                bytes_read_from_result,
                rows_produced,
                rows_inserted,
                rows_updated,
                rows_deleted,
                rows_unloaded,
                bytes_deleted,
                partitions_scanned,
                partitions_total,
                bytes_spilled_to_local_storage,
                bytes_spilled_to_remote_storage,
                bytes_sent_over_the_network,
                compilation_time,
                execution_time,
                queued_provisioning_time,
                queued_repair_time,
                queued_overload_time,
                transaction_blocked_time,
                outbound_data_transfer_cloud,
                outbound_data_transfer_region,
                outbound_data_transfer_bytes,
                inbound_data_transfer_cloud,
                inbound_data_transfer_region,
                inbound_data_transfer_bytes,
                list_external_files_time,
                credits_used_cloud_services,
                external_function_total_invocations,
                external_function_total_sent_rows,
                external_function_total_received_rows,
                external_function_total_sent_bytes,
                external_function_total_received_bytes,
                query_load_percent,
                is_client_generated_statement,
                query_acceleration_bytes_scanned,
                query_acceleration_partitions_scanned,
                query_acceleration_upper_limit_scale_factor,
                transaction_id,
                child_queries_wait_time,
                role_type,
                query_hash,
                query_hash_version,
                secondary_role_stats,
                rows_written_to_result,
                query_retry_time,
                query_retry_cause,
                fault_handling_time    
                FROM snowflake.account_usage.query_history
                WHERE TO_DATE(START_TIME) > DATEADD(MONTH,-1,TO_DATE("""+report_formatted_time+"""))
                AND  NOT CONTAINS (QUERY_TEXT,'*** Project:   https://github.com/prismafy/prismafy  ***')
                AND CONTAINS (QUERY_TEXT,'/* {"app": "dbt"')
                AND query_parameterized_hash IS NOT NULL
        )
        SELECT * 
        FROM DATA        
        WHERE TOP_N<=25
        ORDER BY total_elapsed_time_seg DESC        
        ;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Top dbt models for last month by total_elapsed_time_seg</h3>
        <table class="tabla2">
        <tr>
        <th>top_n</th>
        <th>date</th>
        <th>query_parameterized_hash</th>
        <th>dbt_cloud_run_id</th>
        <th>dbt_version</th>
        <th>dbt_project_name</th>
        <th>dbt_cloud_project_id</th>
        <th>dbt_target_name</th>
        <th>dbt_target_database</th>
        <th>dbt_target_schema</th>
        <th>dbt_node_name</th>
        <th>dbt_node_id</th>
        <th>dbt_materialized</th>
        <th>dbt_node_original_file_path</th>
        <th>total_elapsed_time_seg</th>
        <th>query_id</th>
        <th>database_id</th>
        <th>database_name</th>
        <th>schema_id</th>
        <th>schema_name</th>
        <th>query_type</th>
        <th>session_id</th>
        <th>user_name</th>
        <th>role_name</th>
        <th>warehouse_id</th>
        <th>warehouse_name</th>
        <th>warehouse_size</th>
        <th>warehouse_type</th>
        <th>cluster_number</th>
        <th>query_tag</th>
        <th>execution_status</th>
        <th>error_code</th>
        <th>error_message</th>
        <th>bytes_scanned</th>
        <th>percentage_scanned_from_cache</th>
        <th>bytes_written</th>
        <th>bytes_written_to_result</th>
        <th>bytes_read_from_result</th>
        <th>rows_produced</th>
        <th>rows_inserted</th>
        <th>rows_updated</th>
        <th>rows_deleted</th>
        <th>rows_unloaded</th>
        <th>bytes_deleted</th>
        <th>partitions_scanned</th>
        <th>partitions_total</th>
        <th>bytes_spilled_to_local_storage</th>
        <th>bytes_spilled_to_remote_storage</th>
        <th>bytes_sent_over_the_network</th>
        <th>compilation_time</th>
        <th>execution_time</th>
        <th>queued_provisioning_time</th>
        <th>queued_repair_time</th>
        <th>queued_overload_time</th>
        <th>transaction_blocked_time</th>
        <th>outbound_data_transfer_cloud</th>
        <th>outbound_data_transfer_region</th>
        <th>outbound_data_transfer_bytes</th>
        <th>inbound_data_transfer_cloud</th>
        <th>inbound_data_transfer_region</th>
        <th>inbound_data_transfer_bytes</th>
        <th>list_external_files_time</th>
        <th>credits_used_cloud_services</th>
        <th>external_function_total_invocations</th>
        <th>external_function_total_sent_rows</th>
        <th>external_function_total_received_rows</th>
        <th>external_function_total_sent_bytes</th>
        <th>external_function_total_received_bytes</th>
        <th>query_load_percent</th>
        <th>is_client_generated_statement</th>
        <th>query_acceleration_bytes_scanned</th>
        <th>query_acceleration_partitions_scanned</th>
        <th>query_acceleration_upper_limit_scale_factor</th>
        <th>transaction_id</th>
        <th>child_queries_wait_time</th>
        <th>role_type</th>
        <th>query_hash</th>
        <th>query_hash_version</th>
        <th>secondary_role_stats</th>
        <th>rows_written_to_result</th>
        <th>query_retry_time</th>
        <th>query_retry_cause</th>
        <th>fault_handling_time</th>  
        </tr>  
        """
    
        if int(cur.rowcount)!=0:
            for ( TOP_N, RDATE, QUERY_PARAMETERIZED_HASH, DBT_CLOUD_RUN_ID, DBT_VERSION, DBT_PROJECT_NAME, DBT_CLOUD_PROJECT_ID, DBT_TARGET_NAME, DBT_TARGET_DATABASE, DBT_TARGET_SCHEMA, DBT_NODE_NAME, DBT_NODE_ID, DBT_MATERIALIZED, DBT_NODE_ORIGINAL_FILE_PATH, TOTAL_ELAPSED_TIME_SEG, QUERY_ID, DATABASE_ID, DATABASE_NAME, SCHEMA_ID, SCHEMA_NAME, QUERY_TYPE, SESSION_ID, USER_NAME, ROLE_NAME, WAREHOUSE_ID, WAREHOUSE_NAME, WAREHOUSE_SIZE, WAREHOUSE_TYPE, CLUSTER_NUMBER, QUERY_TAG, EXECUTION_STATUS, ERROR_CODE, ERROR_MESSAGE, BYTES_SCANNED, PERCENTAGE_SCANNED_FROM_CACHE, BYTES_WRITTEN, BYTES_WRITTEN_TO_RESULT, BYTES_READ_FROM_RESULT, ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_UNLOADED, BYTES_DELETED, PARTITIONS_SCANNED, PARTITIONS_TOTAL, BYTES_SPILLED_TO_LOCAL_STORAGE, BYTES_SPILLED_TO_REMOTE_STORAGE, BYTES_SENT_OVER_THE_NETWORK, COMPILATION_TIME, EXECUTION_TIME, QUEUED_PROVISIONING_TIME, QUEUED_REPAIR_TIME, QUEUED_OVERLOAD_TIME, TRANSACTION_BLOCKED_TIME, OUTBOUND_DATA_TRANSFER_CLOUD, OUTBOUND_DATA_TRANSFER_REGION, OUTBOUND_DATA_TRANSFER_BYTES, INBOUND_DATA_TRANSFER_CLOUD, INBOUND_DATA_TRANSFER_REGION, INBOUND_DATA_TRANSFER_BYTES, LIST_EXTERNAL_FILES_TIME, CREDITS_USED_CLOUD_SERVICES, EXTERNAL_FUNCTION_TOTAL_INVOCATIONS, EXTERNAL_FUNCTION_TOTAL_SENT_ROWS, EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS, EXTERNAL_FUNCTION_TOTAL_SENT_BYTES, EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES, QUERY_LOAD_PERCENT, IS_CLIENT_GENERATED_STATEMENT, QUERY_ACCELERATION_BYTES_SCANNED, QUERY_ACCELERATION_PARTITIONS_SCANNED, QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR, TRANSACTION_ID, CHILD_QUERIES_WAIT_TIME, ROLE_TYPE, QUERY_HASH, QUERY_HASH_VERSION, SECONDARY_ROLE_STATS, ROWS_WRITTEN_TO_RESULT, QUERY_RETRY_TIME, QUERY_RETRY_CAUSE, FAULT_HANDLING_TIME ) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(TOP_N)+"""</td> 
                 <td>"""+str(RDATE)+"""</td> 
                 <td>"""+str(QUERY_PARAMETERIZED_HASH)+"""</td> 
                 <td>"""+str(DBT_CLOUD_RUN_ID)+"""</td> 
                 <td>"""+str(DBT_VERSION)+"""</td> 
                 <td>"""+str(DBT_PROJECT_NAME)+"""</td> 
                 <td>"""+str(DBT_CLOUD_PROJECT_ID)+"""</td> 
                 <td>"""+str(DBT_TARGET_NAME)+"""</td> 
                 <td>"""+str(DBT_TARGET_DATABASE)+"""</td> 
                 <td>"""+str(DBT_TARGET_SCHEMA)+"""</td> 
                 <td>"""+str(DBT_NODE_NAME)+"""</td> 
                 <td>"""+str(DBT_NODE_ID)+"""</td> 
                 <td>"""+str(DBT_MATERIALIZED)+"""</td> 
                 <td>"""+str(DBT_NODE_ORIGINAL_FILE_PATH)+"""</td> 
                 <td>"""+str(TOTAL_ELAPSED_TIME_SEG)+"""</td> 
                 <td>"""+str(QUERY_ID)+"""</td> 
                 <td>"""+str(DATABASE_ID)+"""</td> 
                 <td>"""+str(DATABASE_NAME)+"""</td> 
                 <td>"""+str(SCHEMA_ID)+"""</td> 
                 <td>"""+str(SCHEMA_NAME)+"""</td> 
                 <td>"""+str(QUERY_TYPE)+"""</td> 
                 <td>"""+str(SESSION_ID)+"""</td> 
                 <td>"""+str(USER_NAME)+"""</td> 
                 <td>"""+str(ROLE_NAME)+"""</td> 
                 <td>"""+str(WAREHOUSE_ID)+"""</td> 
                 <td>"""+str(WAREHOUSE_NAME)+"""</td> 
                 <td>"""+str(WAREHOUSE_SIZE)+"""</td> 
                 <td>"""+str(WAREHOUSE_TYPE)+"""</td> 
                 <td>"""+str(CLUSTER_NUMBER)+"""</td> 
                 <td>"""+str(QUERY_TAG)+"""</td> 
                 <td>"""+str(EXECUTION_STATUS)+"""</td> 
                 <td>"""+str(ERROR_CODE)+"""</td> 
                 <td>"""+str(ERROR_MESSAGE)+"""</td> 
                 <td>"""+str(BYTES_SCANNED)+"""</td> 
                 <td>"""+str(PERCENTAGE_SCANNED_FROM_CACHE)+"""</td> 
                 <td>"""+str(BYTES_WRITTEN)+"""</td> 
                 <td>"""+str(BYTES_WRITTEN_TO_RESULT)+"""</td>  
                 <td>"""+str(BYTES_READ_FROM_RESULT)+"""</td> 
                 <td>"""+str(ROWS_PRODUCED)+"""</td> 
                 <td>"""+str(ROWS_INSERTED)+"""</td> 
                 <td>"""+str(ROWS_UPDATED)+"""</td> 
                 <td>"""+str(ROWS_DELETED)+"""</td> 
                 <td>"""+str(ROWS_UNLOADED)+"""</td> 
                 <td>"""+str(BYTES_DELETED)+"""</td> 
                 <td>"""+str(PARTITIONS_SCANNED)+"""</td> 
                 <td>"""+str(PARTITIONS_TOTAL)+"""</td> 
                 <td>"""+str(BYTES_SPILLED_TO_LOCAL_STORAGE)+"""</td> 
                 <td>"""+str(BYTES_SPILLED_TO_REMOTE_STORAGE)+"""</td> 
                 <td>"""+str(BYTES_SENT_OVER_THE_NETWORK)+"""</td> 
                 <td>"""+str(COMPILATION_TIME)+"""</td> 
                 <td>"""+str(EXECUTION_TIME)+"""</td> 
                 <td>"""+str(QUEUED_PROVISIONING_TIME)+"""</td> 
                 <td>"""+str(QUEUED_REPAIR_TIME)+"""</td> 
                 <td>"""+str(QUEUED_OVERLOAD_TIME)+"""</td> 
                 <td>"""+str(TRANSACTION_BLOCKED_TIME)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_CLOUD)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_REGION)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_BYTES)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_CLOUD)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_REGION)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_BYTES)+"""</td> 
                 <td>"""+str(LIST_EXTERNAL_FILES_TIME)+"""</td> 
                 <td>"""+str(CREDITS_USED_CLOUD_SERVICES)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_INVOCATIONS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_SENT_ROWS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_SENT_BYTES)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES)+"""</td> 
                 <td>"""+str(QUERY_LOAD_PERCENT)+"""</td> 
                 <td>"""+str(IS_CLIENT_GENERATED_STATEMENT)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_BYTES_SCANNED)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_PARTITIONS_SCANNED)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR)+"""</td> 
                 <td>"""+str(TRANSACTION_ID)+"""</td> 
                 <td>"""+str(CHILD_QUERIES_WAIT_TIME)+"""</td> 
                 <td>"""+str(ROLE_TYPE)+"""</td> 
                 <td>"""+str(QUERY_HASH)+"""</td> 
                 <td>"""+str(QUERY_HASH_VERSION)+"""</td> 
                 <td>"""+str(SECONDARY_ROLE_STATS)+"""</td>  
                 <td>"""+str(ROWS_WRITTEN_TO_RESULT)+"""</td> 
                 <td>"""+str(QUERY_RETRY_TIME)+"""</td> 
                 <td>"""+str(QUERY_RETRY_CAUSE)+"""</td> 
                 <td class="cell_grow">"""+str(FAULT_HANDLING_TIME)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('last_month_top_dbt_models.html',html_file)
        report_sections["H - DBT"].update({'last_month_top_dbt_models.html':'table'})

    except Exception as error:
        print("[table_month_top_dbt_models]: An exception occurred:", error)

def table_week_top_dbt_models(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (                
               SELECT 
                ROW_NUMBER() over (order by total_elapsed_time  DESC)  AS TOP_N,
                START_TIME        AS DATE, 
                query_parameterized_hash, 
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'dbt_cloud_run_id')     
                ELSE NULL  END AS dbt_cloud_run_id,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'dbt_version')     
                ELSE NULL  END AS dbt_version,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'project_name')     
                ELSE NULL  END AS dbt_project_name,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'dbt_cloud_project_id')     
                ELSE NULL  END AS dbt_cloud_project_id,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'target_name')     
                ELSE NULL  END AS dbt_target_name,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'target_database')     
                ELSE NULL  END AS dbt_target_database,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'target_schema')     
                ELSE NULL  END AS dbt_target_schema,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'node_name')     
                ELSE NULL  END AS dbt_node_name,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'node_id')     
                ELSE NULL  END AS dbt_node_id,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'materialized')     
                ELSE NULL  END AS dbt_materialized,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE(REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'node_original_file_path')  
                ELSE NULL  END AS dbt_node_original_file_path,
                ROUND(total_elapsed_time/1000,2)                AS total_elapsed_time_seg,
                query_id,
                database_id,
                database_name,
                schema_id,
                schema_name,
                query_type,
                session_id,
                user_name,
                role_name,
                warehouse_id,
                warehouse_name,
                warehouse_size,
                warehouse_type,
                cluster_number,
                query_tag,
                execution_status,
                error_code,
                error_message,
                bytes_scanned,
                percentage_scanned_from_cache,
                bytes_written,
                bytes_written_to_result,
                bytes_read_from_result,
                rows_produced,
                rows_inserted,
                rows_updated,
                rows_deleted,
                rows_unloaded,
                bytes_deleted,
                partitions_scanned,
                partitions_total,
                bytes_spilled_to_local_storage,
                bytes_spilled_to_remote_storage,
                bytes_sent_over_the_network,
                compilation_time,
                execution_time,
                queued_provisioning_time,
                queued_repair_time,
                queued_overload_time,
                transaction_blocked_time,
                outbound_data_transfer_cloud,
                outbound_data_transfer_region,
                outbound_data_transfer_bytes,
                inbound_data_transfer_cloud,
                inbound_data_transfer_region,
                inbound_data_transfer_bytes,
                list_external_files_time,
                credits_used_cloud_services,
                external_function_total_invocations,
                external_function_total_sent_rows,
                external_function_total_received_rows,
                external_function_total_sent_bytes,
                external_function_total_received_bytes,
                query_load_percent,
                is_client_generated_statement,
                query_acceleration_bytes_scanned,
                query_acceleration_partitions_scanned,
                query_acceleration_upper_limit_scale_factor,
                transaction_id,
                child_queries_wait_time,
                role_type,
                query_hash,
                query_hash_version,
                secondary_role_stats,
                rows_written_to_result,
                query_retry_time,
                query_retry_cause,
                fault_handling_time    
                FROM snowflake.account_usage.query_history
                WHERE TO_DATE(START_TIME) > DATEADD(DAY,-7,TO_DATE("""+report_formatted_time+"""))
                AND  NOT CONTAINS (QUERY_TEXT,'*** Project:   https://github.com/prismafy/prismafy  ***')
                AND CONTAINS (QUERY_TEXT,'/* {"app": "dbt"')
                AND query_parameterized_hash NOT IN ('e66e976d84c0546733a0d35b5f33a84d','e66e976d84c0546733a0d35b5f33a84d','e66e976d84c0546733a0d35b5f33a84d') AND query_parameterized_hash IS NOT NULL
        )
        SELECT * 
        FROM DATA        
        WHERE TOP_N<=25
        ORDER BY total_elapsed_time_seg DESC        
        ;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Top dbt models for last week by total_elapsed_time_seg</h3>
        <table class="tabla2">
        <tr>
        <th>top_n</th>
        <th>date</th>
        <th>query_parameterized_hash</th>
        <th>dbt_cloud_run_id</th>
        <th>dbt_version</th>
        <th>dbt_project_name</th>
        <th>dbt_cloud_project_id</th>
        <th>dbt_target_name</th>
        <th>dbt_target_database</th>
        <th>dbt_target_schema</th>
        <th>dbt_node_name</th>
        <th>dbt_node_id</th>
        <th>dbt_materialized</th>
        <th>dbt_node_original_file_path</th>
        <th>total_elapsed_time_seg</th>
        <th>query_id</th>
        <th>database_id</th>
        <th>database_name</th>
        <th>schema_id</th>
        <th>schema_name</th>
        <th>query_type</th>
        <th>session_id</th>
        <th>user_name</th>
        <th>role_name</th>
        <th>warehouse_id</th>
        <th>warehouse_name</th>
        <th>warehouse_size</th>
        <th>warehouse_type</th>
        <th>cluster_number</th>
        <th>query_tag</th>
        <th>execution_status</th>
        <th>error_code</th>
        <th>error_message</th>
        <th>bytes_scanned</th>
        <th>percentage_scanned_from_cache</th>
        <th>bytes_written</th>
        <th>bytes_written_to_result</th>
        <th>bytes_read_from_result</th>
        <th>rows_produced</th>
        <th>rows_inserted</th>
        <th>rows_updated</th>
        <th>rows_deleted</th>
        <th>rows_unloaded</th>
        <th>bytes_deleted</th>
        <th>partitions_scanned</th>
        <th>partitions_total</th>
        <th>bytes_spilled_to_local_storage</th>
        <th>bytes_spilled_to_remote_storage</th>
        <th>bytes_sent_over_the_network</th>
        <th>compilation_time</th>
        <th>execution_time</th>
        <th>queued_provisioning_time</th>
        <th>queued_repair_time</th>
        <th>queued_overload_time</th>
        <th>transaction_blocked_time</th>
        <th>outbound_data_transfer_cloud</th>
        <th>outbound_data_transfer_region</th>
        <th>outbound_data_transfer_bytes</th>
        <th>inbound_data_transfer_cloud</th>
        <th>inbound_data_transfer_region</th>
        <th>inbound_data_transfer_bytes</th>
        <th>list_external_files_time</th>
        <th>credits_used_cloud_services</th>
        <th>external_function_total_invocations</th>
        <th>external_function_total_sent_rows</th>
        <th>external_function_total_received_rows</th>
        <th>external_function_total_sent_bytes</th>
        <th>external_function_total_received_bytes</th>
        <th>query_load_percent</th>
        <th>is_client_generated_statement</th>
        <th>query_acceleration_bytes_scanned</th>
        <th>query_acceleration_partitions_scanned</th>
        <th>query_acceleration_upper_limit_scale_factor</th>
        <th>transaction_id</th>
        <th>child_queries_wait_time</th>
        <th>role_type</th>
        <th>query_hash</th>
        <th>query_hash_version</th>
        <th>secondary_role_stats</th>
        <th>rows_written_to_result</th>
        <th>query_retry_time</th>
        <th>query_retry_cause</th>
        <th>fault_handling_time</th>  
        </tr>  
        """
    
        if int(cur.rowcount)!=0:
            for ( TOP_N, RDATE, QUERY_PARAMETERIZED_HASH, DBT_CLOUD_RUN_ID, DBT_VERSION, DBT_PROJECT_NAME, DBT_CLOUD_PROJECT_ID, DBT_TARGET_NAME, DBT_TARGET_DATABASE, DBT_TARGET_SCHEMA, DBT_NODE_NAME, DBT_NODE_ID, DBT_MATERIALIZED, DBT_NODE_ORIGINAL_FILE_PATH, TOTAL_ELAPSED_TIME_SEG, QUERY_ID, DATABASE_ID, DATABASE_NAME, SCHEMA_ID, SCHEMA_NAME, QUERY_TYPE, SESSION_ID, USER_NAME, ROLE_NAME, WAREHOUSE_ID, WAREHOUSE_NAME, WAREHOUSE_SIZE, WAREHOUSE_TYPE, CLUSTER_NUMBER, QUERY_TAG, EXECUTION_STATUS, ERROR_CODE, ERROR_MESSAGE, BYTES_SCANNED, PERCENTAGE_SCANNED_FROM_CACHE, BYTES_WRITTEN, BYTES_WRITTEN_TO_RESULT, BYTES_READ_FROM_RESULT, ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_UNLOADED, BYTES_DELETED, PARTITIONS_SCANNED, PARTITIONS_TOTAL, BYTES_SPILLED_TO_LOCAL_STORAGE, BYTES_SPILLED_TO_REMOTE_STORAGE, BYTES_SENT_OVER_THE_NETWORK, COMPILATION_TIME, EXECUTION_TIME, QUEUED_PROVISIONING_TIME, QUEUED_REPAIR_TIME, QUEUED_OVERLOAD_TIME, TRANSACTION_BLOCKED_TIME, OUTBOUND_DATA_TRANSFER_CLOUD, OUTBOUND_DATA_TRANSFER_REGION, OUTBOUND_DATA_TRANSFER_BYTES, INBOUND_DATA_TRANSFER_CLOUD, INBOUND_DATA_TRANSFER_REGION, INBOUND_DATA_TRANSFER_BYTES, LIST_EXTERNAL_FILES_TIME, CREDITS_USED_CLOUD_SERVICES, EXTERNAL_FUNCTION_TOTAL_INVOCATIONS, EXTERNAL_FUNCTION_TOTAL_SENT_ROWS, EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS, EXTERNAL_FUNCTION_TOTAL_SENT_BYTES, EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES, QUERY_LOAD_PERCENT, IS_CLIENT_GENERATED_STATEMENT, QUERY_ACCELERATION_BYTES_SCANNED, QUERY_ACCELERATION_PARTITIONS_SCANNED, QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR, TRANSACTION_ID, CHILD_QUERIES_WAIT_TIME, ROLE_TYPE, QUERY_HASH, QUERY_HASH_VERSION, SECONDARY_ROLE_STATS, ROWS_WRITTEN_TO_RESULT, QUERY_RETRY_TIME, QUERY_RETRY_CAUSE, FAULT_HANDLING_TIME ) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(TOP_N)+"""</td> 
                 <td>"""+str(RDATE)+"""</td> 
                 <td>"""+str(QUERY_PARAMETERIZED_HASH)+"""</td> 
                 <td>"""+str(DBT_CLOUD_RUN_ID)+"""</td> 
                 <td>"""+str(DBT_VERSION)+"""</td> 
                 <td>"""+str(DBT_PROJECT_NAME)+"""</td> 
                 <td>"""+str(DBT_CLOUD_PROJECT_ID)+"""</td> 
                 <td>"""+str(DBT_TARGET_NAME)+"""</td> 
                 <td>"""+str(DBT_TARGET_DATABASE)+"""</td> 
                 <td>"""+str(DBT_TARGET_SCHEMA)+"""</td> 
                 <td>"""+str(DBT_NODE_NAME)+"""</td> 
                 <td>"""+str(DBT_NODE_ID)+"""</td> 
                 <td>"""+str(DBT_MATERIALIZED)+"""</td> 
                 <td>"""+str(DBT_NODE_ORIGINAL_FILE_PATH)+"""</td> 
                 <td>"""+str(TOTAL_ELAPSED_TIME_SEG)+"""</td> 
                 <td>"""+str(QUERY_ID)+"""</td> 
                 <td>"""+str(DATABASE_ID)+"""</td> 
                 <td>"""+str(DATABASE_NAME)+"""</td> 
                 <td>"""+str(SCHEMA_ID)+"""</td> 
                 <td>"""+str(SCHEMA_NAME)+"""</td> 
                 <td>"""+str(QUERY_TYPE)+"""</td> 
                 <td>"""+str(SESSION_ID)+"""</td> 
                 <td>"""+str(USER_NAME)+"""</td> 
                 <td>"""+str(ROLE_NAME)+"""</td> 
                 <td>"""+str(WAREHOUSE_ID)+"""</td> 
                 <td>"""+str(WAREHOUSE_NAME)+"""</td> 
                 <td>"""+str(WAREHOUSE_SIZE)+"""</td> 
                 <td>"""+str(WAREHOUSE_TYPE)+"""</td> 
                 <td>"""+str(CLUSTER_NUMBER)+"""</td> 
                 <td>"""+str(QUERY_TAG)+"""</td> 
                 <td>"""+str(EXECUTION_STATUS)+"""</td> 
                 <td>"""+str(ERROR_CODE)+"""</td> 
                 <td>"""+str(ERROR_MESSAGE)+"""</td> 
                 <td>"""+str(BYTES_SCANNED)+"""</td> 
                 <td>"""+str(PERCENTAGE_SCANNED_FROM_CACHE)+"""</td> 
                 <td>"""+str(BYTES_WRITTEN)+"""</td> 
                 <td>"""+str(BYTES_WRITTEN_TO_RESULT)+"""</td>  
                 <td>"""+str(BYTES_READ_FROM_RESULT)+"""</td> 
                 <td>"""+str(ROWS_PRODUCED)+"""</td> 
                 <td>"""+str(ROWS_INSERTED)+"""</td> 
                 <td>"""+str(ROWS_UPDATED)+"""</td> 
                 <td>"""+str(ROWS_DELETED)+"""</td> 
                 <td>"""+str(ROWS_UNLOADED)+"""</td> 
                 <td>"""+str(BYTES_DELETED)+"""</td> 
                 <td>"""+str(PARTITIONS_SCANNED)+"""</td> 
                 <td>"""+str(PARTITIONS_TOTAL)+"""</td> 
                 <td>"""+str(BYTES_SPILLED_TO_LOCAL_STORAGE)+"""</td> 
                 <td>"""+str(BYTES_SPILLED_TO_REMOTE_STORAGE)+"""</td> 
                 <td>"""+str(BYTES_SENT_OVER_THE_NETWORK)+"""</td> 
                 <td>"""+str(COMPILATION_TIME)+"""</td> 
                 <td>"""+str(EXECUTION_TIME)+"""</td> 
                 <td>"""+str(QUEUED_PROVISIONING_TIME)+"""</td> 
                 <td>"""+str(QUEUED_REPAIR_TIME)+"""</td> 
                 <td>"""+str(QUEUED_OVERLOAD_TIME)+"""</td> 
                 <td>"""+str(TRANSACTION_BLOCKED_TIME)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_CLOUD)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_REGION)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_BYTES)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_CLOUD)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_REGION)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_BYTES)+"""</td> 
                 <td>"""+str(LIST_EXTERNAL_FILES_TIME)+"""</td> 
                 <td>"""+str(CREDITS_USED_CLOUD_SERVICES)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_INVOCATIONS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_SENT_ROWS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_SENT_BYTES)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES)+"""</td> 
                 <td>"""+str(QUERY_LOAD_PERCENT)+"""</td> 
                 <td>"""+str(IS_CLIENT_GENERATED_STATEMENT)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_BYTES_SCANNED)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_PARTITIONS_SCANNED)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR)+"""</td> 
                 <td>"""+str(TRANSACTION_ID)+"""</td> 
                 <td>"""+str(CHILD_QUERIES_WAIT_TIME)+"""</td> 
                 <td>"""+str(ROLE_TYPE)+"""</td> 
                 <td>"""+str(QUERY_HASH)+"""</td> 
                 <td>"""+str(QUERY_HASH_VERSION)+"""</td> 
                 <td>"""+str(SECONDARY_ROLE_STATS)+"""</td>  
                 <td>"""+str(ROWS_WRITTEN_TO_RESULT)+"""</td> 
                 <td>"""+str(QUERY_RETRY_TIME)+"""</td> 
                 <td>"""+str(QUERY_RETRY_CAUSE)+"""</td> 
                 <td class="cell_grow">"""+str(FAULT_HANDLING_TIME)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('last_week_top_dbt_models.html',html_file)
        report_sections["H - DBT"].update({'last_week_top_dbt_models.html':'table'})

    except Exception as error:
        print("[table_week_top_dbt_models]: An exception occurred:", error)

def table_history_top_dbt_models(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (                
               SELECT 
                ROW_NUMBER() over (order by total_elapsed_time  DESC)  AS TOP_N,
                START_TIME        AS DATE, 
                query_parameterized_hash, 
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'dbt_cloud_run_id')     
                ELSE NULL  END AS dbt_cloud_run_id,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'dbt_version')     
                ELSE NULL  END AS dbt_version,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'project_name')     
                ELSE NULL  END AS dbt_project_name,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'dbt_cloud_project_id')     
                ELSE NULL  END AS dbt_cloud_project_id,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'target_name')     
                ELSE NULL  END AS dbt_target_name,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'target_database')     
                ELSE NULL  END AS dbt_target_database,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'target_schema')     
                ELSE NULL  END AS dbt_target_schema,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'node_name')     
                ELSE NULL  END AS dbt_node_name,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'node_id')     
                ELSE NULL  END AS dbt_node_id,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE (REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'materialized')     
                ELSE NULL  END AS dbt_materialized,
                CASE WHEN position('/* {"app": "dbt"', query_text, 1)>0 THEN 
                JSON_EXTRACT_PATH_TEXT(REPLACE(REPLACE(SUBSTR(query_text, position('/* {"app": "dbt"', query_text,1)+3),'*/;'),'*/'), 'node_original_file_path')  
                ELSE NULL  END AS dbt_node_original_file_path,
                ROUND(total_elapsed_time/1000,2)                AS total_elapsed_time_seg,
                query_id,
                database_id,
                database_name,
                schema_id,
                schema_name,
                query_type,
                session_id,
                user_name,
                role_name,
                warehouse_id,
                warehouse_name,
                warehouse_size,
                warehouse_type,
                cluster_number,
                query_tag,
                execution_status,
                error_code,
                error_message,
                bytes_scanned,
                percentage_scanned_from_cache,
                bytes_written,
                bytes_written_to_result,
                bytes_read_from_result,
                rows_produced,
                rows_inserted,
                rows_updated,
                rows_deleted,
                rows_unloaded,
                bytes_deleted,
                partitions_scanned,
                partitions_total,
                bytes_spilled_to_local_storage,
                bytes_spilled_to_remote_storage,
                bytes_sent_over_the_network,
                compilation_time,
                execution_time,
                queued_provisioning_time,
                queued_repair_time,
                queued_overload_time,
                transaction_blocked_time,
                outbound_data_transfer_cloud,
                outbound_data_transfer_region,
                outbound_data_transfer_bytes,
                inbound_data_transfer_cloud,
                inbound_data_transfer_region,
                inbound_data_transfer_bytes,
                list_external_files_time,
                credits_used_cloud_services,
                external_function_total_invocations,
                external_function_total_sent_rows,
                external_function_total_received_rows,
                external_function_total_sent_bytes,
                external_function_total_received_bytes,
                query_load_percent,
                is_client_generated_statement,
                query_acceleration_bytes_scanned,
                query_acceleration_partitions_scanned,
                query_acceleration_upper_limit_scale_factor,
                transaction_id,
                child_queries_wait_time,
                role_type,
                query_hash,
                query_hash_version,
                secondary_role_stats,
                rows_written_to_result,
                query_retry_time,
                query_retry_cause,
                fault_handling_time    
                FROM snowflake.account_usage.query_history
                WHERE TO_DATE(START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_DATE("""+report_formatted_time+"""))
                AND  NOT CONTAINS (QUERY_TEXT,'*** Project:   https://github.com/prismafy/prismafy  ***')
                AND CONTAINS (QUERY_TEXT,'/* {"app": "dbt"')
                AND query_parameterized_hash NOT IN ('e66e976d84c0546733a0d35b5f33a84d','e66e976d84c0546733a0d35b5f33a84d','e66e976d84c0546733a0d35b5f33a84d') AND query_parameterized_hash IS NOT NULL
        )
        SELECT * 
        FROM DATA        
        WHERE TOP_N<=100
        ORDER BY total_elapsed_time_seg DESC        
        ;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Top dbt models by total_elapsed_time_seg</h3>
        <table class="tabla2">
        <tr>
        <th>top_n</th>
        <th>date</th>
        <th>query_parameterized_hash</th>
        <th>dbt_cloud_run_id</th>
        <th>dbt_version</th>
        <th>dbt_project_name</th>
        <th>dbt_cloud_project_id</th>
        <th>dbt_target_name</th>
        <th>dbt_target_database</th>
        <th>dbt_target_schema</th>
        <th>dbt_node_name</th>
        <th>dbt_node_id</th>
        <th>dbt_materialized</th>
        <th>dbt_node_original_file_path</th>
        <th>total_elapsed_time_seg</th>
        <th>query_id</th>
        <th>database_id</th>
        <th>database_name</th>
        <th>schema_id</th>
        <th>schema_name</th>
        <th>query_type</th>
        <th>session_id</th>
        <th>user_name</th>
        <th>role_name</th>
        <th>warehouse_id</th>
        <th>warehouse_name</th>
        <th>warehouse_size</th>
        <th>warehouse_type</th>
        <th>cluster_number</th>
        <th>query_tag</th>
        <th>execution_status</th>
        <th>error_code</th>
        <th>error_message</th>
        <th>bytes_scanned</th>
        <th>percentage_scanned_from_cache</th>
        <th>bytes_written</th>
        <th>bytes_written_to_result</th>
        <th>bytes_read_from_result</th>
        <th>rows_produced</th>
        <th>rows_inserted</th>
        <th>rows_updated</th>
        <th>rows_deleted</th>
        <th>rows_unloaded</th>
        <th>bytes_deleted</th>
        <th>partitions_scanned</th>
        <th>partitions_total</th>
        <th>bytes_spilled_to_local_storage</th>
        <th>bytes_spilled_to_remote_storage</th>
        <th>bytes_sent_over_the_network</th>
        <th>compilation_time</th>
        <th>execution_time</th>
        <th>queued_provisioning_time</th>
        <th>queued_repair_time</th>
        <th>queued_overload_time</th>
        <th>transaction_blocked_time</th>
        <th>outbound_data_transfer_cloud</th>
        <th>outbound_data_transfer_region</th>
        <th>outbound_data_transfer_bytes</th>
        <th>inbound_data_transfer_cloud</th>
        <th>inbound_data_transfer_region</th>
        <th>inbound_data_transfer_bytes</th>
        <th>list_external_files_time</th>
        <th>credits_used_cloud_services</th>
        <th>external_function_total_invocations</th>
        <th>external_function_total_sent_rows</th>
        <th>external_function_total_received_rows</th>
        <th>external_function_total_sent_bytes</th>
        <th>external_function_total_received_bytes</th>
        <th>query_load_percent</th>
        <th>is_client_generated_statement</th>
        <th>query_acceleration_bytes_scanned</th>
        <th>query_acceleration_partitions_scanned</th>
        <th>query_acceleration_upper_limit_scale_factor</th>
        <th>transaction_id</th>
        <th>child_queries_wait_time</th>
        <th>role_type</th>
        <th>query_hash</th>
        <th>query_hash_version</th>
        <th>secondary_role_stats</th>
        <th>rows_written_to_result</th>
        <th>query_retry_time</th>
        <th>query_retry_cause</th>
        <th>fault_handling_time</th>  
        </tr>  
        """
    
        if int(cur.rowcount)!=0:
            for ( TOP_N, RDATE, QUERY_PARAMETERIZED_HASH, DBT_CLOUD_RUN_ID, DBT_VERSION, DBT_PROJECT_NAME, DBT_CLOUD_PROJECT_ID, DBT_TARGET_NAME, DBT_TARGET_DATABASE, DBT_TARGET_SCHEMA, DBT_NODE_NAME, DBT_NODE_ID, DBT_MATERIALIZED, DBT_NODE_ORIGINAL_FILE_PATH, TOTAL_ELAPSED_TIME_SEG, QUERY_ID, DATABASE_ID, DATABASE_NAME, SCHEMA_ID, SCHEMA_NAME, QUERY_TYPE, SESSION_ID, USER_NAME, ROLE_NAME, WAREHOUSE_ID, WAREHOUSE_NAME, WAREHOUSE_SIZE, WAREHOUSE_TYPE, CLUSTER_NUMBER, QUERY_TAG, EXECUTION_STATUS, ERROR_CODE, ERROR_MESSAGE, BYTES_SCANNED, PERCENTAGE_SCANNED_FROM_CACHE, BYTES_WRITTEN, BYTES_WRITTEN_TO_RESULT, BYTES_READ_FROM_RESULT, ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_UNLOADED, BYTES_DELETED, PARTITIONS_SCANNED, PARTITIONS_TOTAL, BYTES_SPILLED_TO_LOCAL_STORAGE, BYTES_SPILLED_TO_REMOTE_STORAGE, BYTES_SENT_OVER_THE_NETWORK, COMPILATION_TIME, EXECUTION_TIME, QUEUED_PROVISIONING_TIME, QUEUED_REPAIR_TIME, QUEUED_OVERLOAD_TIME, TRANSACTION_BLOCKED_TIME, OUTBOUND_DATA_TRANSFER_CLOUD, OUTBOUND_DATA_TRANSFER_REGION, OUTBOUND_DATA_TRANSFER_BYTES, INBOUND_DATA_TRANSFER_CLOUD, INBOUND_DATA_TRANSFER_REGION, INBOUND_DATA_TRANSFER_BYTES, LIST_EXTERNAL_FILES_TIME, CREDITS_USED_CLOUD_SERVICES, EXTERNAL_FUNCTION_TOTAL_INVOCATIONS, EXTERNAL_FUNCTION_TOTAL_SENT_ROWS, EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS, EXTERNAL_FUNCTION_TOTAL_SENT_BYTES, EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES, QUERY_LOAD_PERCENT, IS_CLIENT_GENERATED_STATEMENT, QUERY_ACCELERATION_BYTES_SCANNED, QUERY_ACCELERATION_PARTITIONS_SCANNED, QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR, TRANSACTION_ID, CHILD_QUERIES_WAIT_TIME, ROLE_TYPE, QUERY_HASH, QUERY_HASH_VERSION, SECONDARY_ROLE_STATS, ROWS_WRITTEN_TO_RESULT, QUERY_RETRY_TIME, QUERY_RETRY_CAUSE, FAULT_HANDLING_TIME ) in cur:
                html_file=html_file+""" <tr> 
                 <td>"""+str(TOP_N)+"""</td> 
                 <td>"""+str(RDATE)+"""</td> 
                 <td>"""+str(QUERY_PARAMETERIZED_HASH)+"""</td> 
                 <td>"""+str(DBT_CLOUD_RUN_ID)+"""</td> 
                 <td>"""+str(DBT_VERSION)+"""</td> 
                 <td>"""+str(DBT_PROJECT_NAME)+"""</td> 
                 <td>"""+str(DBT_CLOUD_PROJECT_ID)+"""</td> 
                 <td>"""+str(DBT_TARGET_NAME)+"""</td> 
                 <td>"""+str(DBT_TARGET_DATABASE)+"""</td> 
                 <td>"""+str(DBT_TARGET_SCHEMA)+"""</td> 
                 <td>"""+str(DBT_NODE_NAME)+"""</td> 
                 <td>"""+str(DBT_NODE_ID)+"""</td> 
                 <td>"""+str(DBT_MATERIALIZED)+"""</td> 
                 <td>"""+str(DBT_NODE_ORIGINAL_FILE_PATH)+"""</td> 
                 <td>"""+str(TOTAL_ELAPSED_TIME_SEG)+"""</td> 
                 <td>"""+str(QUERY_ID)+"""</td> 
                 <td>"""+str(DATABASE_ID)+"""</td> 
                 <td>"""+str(DATABASE_NAME)+"""</td> 
                 <td>"""+str(SCHEMA_ID)+"""</td> 
                 <td>"""+str(SCHEMA_NAME)+"""</td> 
                 <td>"""+str(QUERY_TYPE)+"""</td> 
                 <td>"""+str(SESSION_ID)+"""</td> 
                 <td>"""+str(USER_NAME)+"""</td> 
                 <td>"""+str(ROLE_NAME)+"""</td> 
                 <td>"""+str(WAREHOUSE_ID)+"""</td> 
                 <td>"""+str(WAREHOUSE_NAME)+"""</td> 
                 <td>"""+str(WAREHOUSE_SIZE)+"""</td> 
                 <td>"""+str(WAREHOUSE_TYPE)+"""</td> 
                 <td>"""+str(CLUSTER_NUMBER)+"""</td> 
                 <td>"""+str(QUERY_TAG)+"""</td> 
                 <td>"""+str(EXECUTION_STATUS)+"""</td> 
                 <td>"""+str(ERROR_CODE)+"""</td> 
                 <td>"""+str(ERROR_MESSAGE)+"""</td> 
                 <td>"""+str(BYTES_SCANNED)+"""</td> 
                 <td>"""+str(PERCENTAGE_SCANNED_FROM_CACHE)+"""</td> 
                 <td>"""+str(BYTES_WRITTEN)+"""</td> 
                 <td>"""+str(BYTES_WRITTEN_TO_RESULT)+"""</td>  
                 <td>"""+str(BYTES_READ_FROM_RESULT)+"""</td> 
                 <td>"""+str(ROWS_PRODUCED)+"""</td> 
                 <td>"""+str(ROWS_INSERTED)+"""</td> 
                 <td>"""+str(ROWS_UPDATED)+"""</td> 
                 <td>"""+str(ROWS_DELETED)+"""</td> 
                 <td>"""+str(ROWS_UNLOADED)+"""</td> 
                 <td>"""+str(BYTES_DELETED)+"""</td> 
                 <td>"""+str(PARTITIONS_SCANNED)+"""</td> 
                 <td>"""+str(PARTITIONS_TOTAL)+"""</td> 
                 <td>"""+str(BYTES_SPILLED_TO_LOCAL_STORAGE)+"""</td> 
                 <td>"""+str(BYTES_SPILLED_TO_REMOTE_STORAGE)+"""</td> 
                 <td>"""+str(BYTES_SENT_OVER_THE_NETWORK)+"""</td> 
                 <td>"""+str(COMPILATION_TIME)+"""</td> 
                 <td>"""+str(EXECUTION_TIME)+"""</td> 
                 <td>"""+str(QUEUED_PROVISIONING_TIME)+"""</td> 
                 <td>"""+str(QUEUED_REPAIR_TIME)+"""</td> 
                 <td>"""+str(QUEUED_OVERLOAD_TIME)+"""</td> 
                 <td>"""+str(TRANSACTION_BLOCKED_TIME)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_CLOUD)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_REGION)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_BYTES)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_CLOUD)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_REGION)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_BYTES)+"""</td> 
                 <td>"""+str(LIST_EXTERNAL_FILES_TIME)+"""</td> 
                 <td>"""+str(CREDITS_USED_CLOUD_SERVICES)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_INVOCATIONS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_SENT_ROWS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_SENT_BYTES)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES)+"""</td> 
                 <td>"""+str(QUERY_LOAD_PERCENT)+"""</td> 
                 <td>"""+str(IS_CLIENT_GENERATED_STATEMENT)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_BYTES_SCANNED)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_PARTITIONS_SCANNED)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR)+"""</td> 
                 <td>"""+str(TRANSACTION_ID)+"""</td> 
                 <td>"""+str(CHILD_QUERIES_WAIT_TIME)+"""</td> 
                 <td>"""+str(ROLE_TYPE)+"""</td> 
                 <td>"""+str(QUERY_HASH)+"""</td> 
                 <td>"""+str(QUERY_HASH_VERSION)+"""</td> 
                 <td>"""+str(SECONDARY_ROLE_STATS)+"""</td>  
                 <td>"""+str(ROWS_WRITTEN_TO_RESULT)+"""</td> 
                 <td>"""+str(QUERY_RETRY_TIME)+"""</td> 
                 <td>"""+str(QUERY_RETRY_CAUSE)+"""</td> 
                 <td class="cell_grow">"""+str(FAULT_HANDLING_TIME)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('history_top_dbt_models.html',html_file)
        report_sections["H - DBT"].update({'history_top_dbt_models.html':'table'})

    except Exception as error:
        print("[table_history_top_dbt_models]: An exception occurred:", error)

def line_history_sql_operations(conn):
    try:
        
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
            
        html_file=html_header

        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT 
                query_type                                                  AS QUERY_TYPE
            FROM snowflake.account_usage.query_history Q
            WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND DATABASE_NAME !='SNOWFLAKE'
        )
        SELECT DISTINCT QUERY_TYPE
        FROM DATA  
        """
        cur = conn.cursor()
        cur.execute(sql_query)

        column_count=cur.rowcount        
        headers =[i[0] for i in cur]  
        headers.insert(0,'DATE')  
        html_file=html_file+str(headers).replace('"','')+", \n"      


        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('HOUR',START_TIME::TIMESTAMP_NTZ)         AS DATE , 
            query_type                                                  AS QUERY_TYPE,
            COUNT(*)                                                    AS EXECUTIONS
            FROM snowflake.account_usage.query_history Q
            WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND DATABASE_NAME !='SNOWFLAKE'
            AND DATABASE_NAME IS NOT NULL
            GROUP BY 1,2
            ORDER BY 1  
        )
        , PIVOT_DATA AS (
            SELECT *
            FROM DATA
                PIVOT(  sum (EXECUTIONS) FOR query_type IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) ) 
        )
        SELECT ARRAY_CONSTRUCT(*) FROM PIVOT_DATA ORDER BY DATE
        """

        cur = conn.cursor()
        cur.execute(sql_query)
        
        if int(cur.rowcount)!=0:
            for row  in cur:
                html_file=html_file+str(row[0])+""","""
            
            
            html_file=html_file+html_body1
                
            for i in range(1, column_count):
                if i==column_count-1:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""
            
            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            SQL Operations in the history`,"""+html_line_hour_tail

            create_output_file('history_sql_operations.html',html_file)
            report_sections["G - Maintenance"].update({'history_sql_operations.html':'line'})
        
    except Exception as error:
        print("[line_history_sql_operations]: An exception occurred:", error)

def line_history_sql_operations_by_database(conn):
    try:            
        global snowflake_conn
        global report_sections
        conn =snowflake_conn

        sql_query_dbs=sql_header+"""
        WITH DATA AS (
            SELECT 
                DATABASE_NAME
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
            WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND DATABASE_NAME IS NOT NULL
            AND DATABASE_NAME !='SNOWFLAKE'
        )
        SELECT DISTINCT DATABASE_NAME
        FROM DATA  
        """
        cur_db = conn.cursor()
        cur_db.execute(sql_query_dbs)

        if int(cur_db.rowcount)!=0:
            for database_name  in cur_db:

                html_file=html_header

                sql_query=sql_header+"""
                WITH DATA AS (
                    SELECT 
                        query_type                                                  AS QUERY_TYPE
                    FROM snowflake.account_usage.query_history Q
                    WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                    AND DATABASE_NAME='"""+database_name[0]+"""'
                )
                SELECT DISTINCT QUERY_TYPE
                FROM DATA  
                """
                cur = conn.cursor()
                cur.execute(sql_query)

                column_count=cur.rowcount        
                headers =[i[0] for i in cur]  
                headers.insert(0,'DATE')  
                html_file=html_file+str(headers).replace('"','')+", \n"      


                sql_query=sql_header+"""
                WITH DATA AS (
                    SELECT DATE_TRUNC('HOUR',START_TIME::TIMESTAMP_NTZ)         AS DATE , 
                    query_type                                                  AS QUERY_TYPE,
                    COUNT(*)                                                    AS EXECUTIONS
                    FROM snowflake.account_usage.query_history Q
                    WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                    AND DATABASE_NAME='"""+database_name[0]+"""'
                    GROUP BY 1,2
                    ORDER BY 1  
                )
                , PIVOT_DATA AS (
                    SELECT *
                    FROM DATA
                        PIVOT(  sum (EXECUTIONS) FOR query_type IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) ) 
                )
                SELECT ARRAY_CONSTRUCT(*) FROM PIVOT_DATA ORDER BY DATE
                """

                cur = conn.cursor()
                cur.execute(sql_query)
                
                if int(cur.rowcount)!=0:
                    for row  in cur:
                        html_file=html_file+str(row[0])+""","""
                    
                    
                html_file=html_file+html_body1
                    
                for i in range(1, column_count):
                    if i==column_count-1:
                        html_file=html_file+"""row["""+str(i)+"""]"""
                    else:
                        html_file=html_file+"""row["""+str(i)+"""],"""
                
                html_file=html_file+html_body2+"""
                title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
                Chart Creation Date: """+report_formatted_time+"""
                SQL Operations for database """+database_name[0]+"""`,"""+html_line_hour_tail

                create_output_file('history_sql_operations_for_db_'+database_name[0].lower()+'.html',html_file)
                report_sections["G - Maintenance"].update({'history_sql_operations_for_db_'+database_name[0].lower()+'.html':'line'})
                
    except Exception as error:
        print("[line_history_sql_operations_by_database]: An exception occurred:", error)

def table_history_warehouse_events(conn,warehouse_name):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        
        html_file=html_table_header+"""
        <h3>Warehouse events</h3>
        <table class="tabla2">
        <tr>
        <th>TIMESTAMP</th>
        <th>WAREHOUSE_ID</th>
        <th>WAREHOUSE_NAME</th>
        <th>CLUSTER_NUMBER</th>
        <th>EVENT_NAME</th>
        <th>EVENT_REASON</th>
        <th>EVENT_STATE</th>
        <th>USER_NAME</th>
        <th>ROLE_NAME</th>
        <th>QUERY_ID</th>
        <th>SIZE</th>
        <th>CLUSTER_COUNT</th>
        """

        sql_query_details=sql_header+"""
        SELECT 
            TIMESTAMP,
            WAREHOUSE_ID,
            WAREHOUSE_NAME,
            CLUSTER_NUMBER,
            EVENT_NAME,
            EVENT_REASON,
            EVENT_STATE,
            USER_NAME,
            ROLE_NAME,
            QUERY_ID,
            SIZE,
            CLUSTER_COUNT
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY  
        WHERE  WAREHOUSE_NAME='"""+str(warehouse_name)+"""'
        ORDER BY TIMESTAMP DESC
        LIMIT 100;
        """
        
        cur_details = conn.cursor()
        cur_details.execute(sql_query_details)
    
        if int(cur_details.rowcount)!=0:
            for ( TIMESTAMP, WAREHOUSE_ID, WAREHOUSE_NAME, CLUSTER_NUMBER, EVENT_NAME, EVENT_REASON, EVENT_STATE, USER_NAME, ROLE_NAME, QUERY_ID, SIZE, CLUSTER_COUNT) in cur_details:
                html_file=html_file+""" <tr> 
                <td>"""+str(TIMESTAMP)+"""</td> 
                <td>"""+str(WAREHOUSE_ID)+"""</td> 
                <td>"""+str(WAREHOUSE_NAME)+"""</td> 
                <td>"""+str(CLUSTER_NUMBER)+"""</td> 
                <td>"""+str(EVENT_NAME)+"""</td> 
                <td>"""+str(EVENT_REASON)+"""</td> 
                <td>"""+str(EVENT_STATE)+"""</td> 
                <td>"""+str(USER_NAME)+"""</td> 
                <td>"""+str(ROLE_NAME)+"""</td> 
                <td>"""+str(QUERY_ID)+"""</td> 
                <td>"""+str(SIZE)+"""</td> 
                <td class="cell_grow">"""+str(CLUSTER_COUNT)+"""</td> 
                </tr> """
    
        html_file=html_file+html_table_tail
    
        create_output_file('warehouse_events_for_'+str(warehouse_name.lower())+'.html',html_file)
        report_sections["A - Computing"].update({'warehouse_events_for_'+str(warehouse_name.lower())+'.html':'table'})

    except Exception as error:
        print("[table_history_warehouse_events]: An exception occurred:", error)

def line_history_warehouse_enable_vs_querycount(conn,warehouse_name):
    try:            
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        html_file=html_header

        sql_query="""
        WITH RECURSIVE CALENDAR_MINUTES AS (
        SELECT DATE_TRUNC('MINUTE',DATEADD (MONTH,"""+months_history+""",CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ)) AS calendar_date
        UNION ALL
        SELECT DATE_TRUNC('MINUTE', DATEADD(HOUR, 3, calendar_date))
        FROM CALENDAR_MINUTES
        WHERE calendar_date < CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
        )
        ,QUERY_HIST AS (
            SELECT
            DATE_TRUNC('MINUTE',INTERVAL_START_TIME::TIMESTAMP_NTZ)  AS DATE,
            COUNT(*)                                        AS QUERY_COUNT
            FROM SNOWFLAKE.ACCOUNT_USAGE.AGGREGATE_QUERY_HISTORY 
            WHERE  TO_DATE(INTERVAL_START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND WAREHOUSE_NAME='"""+warehouse_name+"""'
            GROUP BY 1
        )    
        , MAX_QUERY_COUNT AS (
            SELECT 
                MAX(QUERY_COUNT) MAX_QUERY_COUNT 
            FROM QUERY_HIST
        )
        , WAREHOUSE_EVENTS AS (
            SELECT 
            DATE_TRUNC('MINUTE',TIMESTAMP::TIMESTAMP_NTZ)   AS DATE,                                                    
            CASE WHEN E.EVENT_NAME='RESUME_WAREHOUSE' THEN 
                MAX_QUERY_COUNT.MAX_QUERY_COUNT 
            ELSE 0 
            END     ACTIVE_WAREHOUSE
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY  E, MAX_QUERY_COUNT
            WHERE E.EVENT_NAME IN ('RESUME_WAREHOUSE','SUSPEND_WAREHOUSE')
            AND  TO_DATE(TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND WAREHOUSE_NAME='"""+warehouse_name+"""'                                                                           
        )    
        , DATA_JOIN AS (
            SELECT 
                COALESCE (E.DATE,H.DATE,CM.calendar_date) AS DATE, 
                E.ACTIVE_WAREHOUSE,
                H.QUERY_COUNT
            FROM WAREHOUSE_EVENTS E FULL OUTER  JOIN QUERY_HIST H
            ON (E.DATE=H.DATE)
            FULL OUTER  JOIN CALENDAR_MINUTES CM
            ON (CM.calendar_date=COALESCE (E.DATE,H.DATE))
        )   
       , WH_NO_GAPS AS (
            SELECT 
                DATE, 
                NVL(ACTIVE_WAREHOUSE , LAG (ACTIVE_WAREHOUSE,1,NULL)  IGNORE NULLS OVER (ORDER BY DATE) )  ACTIVE_WAREHOUSE,
                QUERY_COUNT 
            FROM DATA_JOIN 
        )
        , DATA_NO_GAPS AS (
            SELECT 
                DATE, 
               ACTIVE_WAREHOUSE, 
               CASE WHEN ACTIVE_WAREHOUSE>0 AND ACTIVE_WAREHOUSE IS NOT NULL THEN 
                   NVL(QUERY_COUNT , LAG (QUERY_COUNT,1,NULL)  IGNORE NULLS OVER (ORDER BY DATE) )  
               ELSE 0
               END QUERY_COUNT, 
            FROM WH_NO_GAPS
        )
        , SUMMARY_DATA AS (
        SELECT
            DATE,
            NVL(ACTIVE_WAREHOUSE,0) ACTIVE_WAREHOUSE,
            NVL(QUERY_COUNT,0) QUERY_COUNT,
            CASE WHEN ACTIVE_WAREHOUSE>0 AND QUERY_COUNT=0 THEN 
                ACTIVE_WAREHOUSE
            ELSE 0
            END ACTIVE_WH_WITHOUT_LOAD
        FROM 
            DATA_NO_GAPS
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA 
        FROM SUMMARY_DATA
        ORDER BY DATE;
        """        
        cur = conn.cursor()
        cur.execute(sql_query)

        headers ="['DATE', 'ACTIVE_WAREHOUSE', 'QUERY_COUNT','ACTIVE_WH_WITHOUT_LOAD']"+", \n"      
        html_file=html_file+headers
        
        if int(cur.rowcount)!=0:
            for row  in cur:
                html_file=html_file+str(row[0]).replace("undefined","null")+""","""
            
            
            html_file=html_file+html_body1+"""
                    row[1],row[2],row[3]
                    """+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Warehouse Active vs Query Count for """+warehouse_name+"""
            Detect those periods when your warehouse is active and it does not have load`,"""+html_stepped_area_minute_tail

            create_output_file('enable_vs_querycount_for_warehouse_'+warehouse_name.lower()+'.html',html_file)
            report_sections["A - Computing"].update({'enable_vs_querycount_for_warehouse_'+warehouse_name.lower()+'.html':'line'})
                
    except Exception as error:
        print("[line_history_warehouse_enable_vs_querycount]: An exception occurred:", error)

def table_last_executions_of_query(conn,sql_query_id):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail  
        global report_sections     
        conn =snowflake_conn            
        html_file=html_table_header    
        table_explain_by_query(conn,sql_query_id)

        sql_query=sql_header+"""
        WITH DATA AS (                
               SELECT 
                START_TIME::TIMESTAMP_NTZ         AS DATE, 
                query_parameterized_hash,                 
                ROUND(total_elapsed_time/1000,2)                AS total_elapsed_time_seg,
                nvl( round( 100 * ( total_elapsed_time - lag(total_elapsed_time, 1) over ( partition by query_parameterized_hash ORDER BY START_TIME ) )/ lag(total_elapsed_time, 1) over ( partition by query_parameterized_hash ORDER BY START_TIME ), 2), 0 ) as growth,
                query_id,
                database_id,
                database_name,
                schema_id,
                schema_name,
                query_type,
                session_id,
                user_name,
                role_name,
                warehouse_id,
                warehouse_name,
                warehouse_size,
                warehouse_type,
                cluster_number,
                query_tag,
                execution_status,
                error_code,
                error_message,
                bytes_scanned,
                percentage_scanned_from_cache,
                bytes_written,
                bytes_written_to_result,
                bytes_read_from_result,
                rows_produced,
                rows_inserted,
                rows_updated,
                rows_deleted,
                rows_unloaded,
                bytes_deleted,
                partitions_scanned,
                partitions_total,
                bytes_spilled_to_local_storage,
                bytes_spilled_to_remote_storage,
                bytes_sent_over_the_network,
                compilation_time,
                execution_time,
                queued_provisioning_time,
                queued_repair_time,
                queued_overload_time,
                transaction_blocked_time,
                outbound_data_transfer_cloud,
                outbound_data_transfer_region,
                outbound_data_transfer_bytes,
                inbound_data_transfer_cloud,
                inbound_data_transfer_region,
                inbound_data_transfer_bytes,
                list_external_files_time,
                credits_used_cloud_services,
                external_function_total_invocations,
                external_function_total_sent_rows,
                external_function_total_received_rows,
                external_function_total_sent_bytes,
                external_function_total_received_bytes,
                query_load_percent,
                is_client_generated_statement,
                query_acceleration_bytes_scanned,
                query_acceleration_partitions_scanned,
                query_acceleration_upper_limit_scale_factor,
                transaction_id,
                child_queries_wait_time,
                role_type,
                query_hash,
                query_hash_version,
                secondary_role_stats,
                rows_written_to_result,
                query_retry_time,
                query_retry_cause,
                fault_handling_time ,
                REPLACE(REPLACE(QUERY_TEXT,'\n',' '),'<td>')  QUERY_TEXT,
                FROM snowflake.account_usage.query_history
                WHERE TO_DATE(START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_DATE("""+report_formatted_time+"""))
                AND query_parameterized_hash='"""+sql_query_id+"""'
        )
        SELECT * 
        FROM DATA        
        ORDER BY DATE DESC        
        LIMIT 100
        ;
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Last executions for query """+sql_query_id+"""</h3>
        <table class="tabla2">
        <tr>
        <th>date</th>
        <th>query_parameterized_hash</th>
        <th>total_elapsed_time_seg</th>
        <th>growth%</th>        
        <th>query_id</th>
        <th>execution_plan_hash</th>
        <th>database_id</th>
        <th>database_name</th>
        <th>schema_id</th>
        <th>schema_name</th>
        <th>query_type</th>
        <th>session_id</th>
        <th>user_name</th>
        <th>role_name</th>
        <th>warehouse_id</th>
        <th>warehouse_name</th>
        <th>warehouse_size</th>
        <th>warehouse_type</th>
        <th>cluster_number</th>
        <th>query_tag</th>
        <th>execution_status</th>
        <th>error_code</th>
        <th>error_message</th>
        <th>bytes_scanned</th>
        <th>percentage_scanned_from_cache</th>
        <th>bytes_written</th>
        <th>bytes_written_to_result</th>
        <th>bytes_read_from_result</th>
        <th>rows_produced</th>
        <th>rows_inserted</th>
        <th>rows_updated</th>
        <th>rows_deleted</th>
        <th>rows_unloaded</th>
        <th>bytes_deleted</th>
        <th>partitions_scanned</th>
        <th>partitions_total</th>
        <th>bytes_spilled_to_local_storage</th>
        <th>bytes_spilled_to_remote_storage</th>
        <th>bytes_sent_over_the_network</th>
        <th>compilation_time</th>
        <th>execution_time</th>
        <th>queued_provisioning_time</th>
        <th>queued_repair_time</th>
        <th>queued_overload_time</th>
        <th>transaction_blocked_time</th>
        <th>outbound_data_transfer_cloud</th>
        <th>outbound_data_transfer_region</th>
        <th>outbound_data_transfer_bytes</th>
        <th>inbound_data_transfer_cloud</th>
        <th>inbound_data_transfer_region</th>
        <th>inbound_data_transfer_bytes</th>
        <th>list_external_files_time</th>
        <th>credits_used_cloud_services</th>
        <th>external_function_total_invocations</th>
        <th>external_function_total_sent_rows</th>
        <th>external_function_total_received_rows</th>
        <th>external_function_total_sent_bytes</th>
        <th>external_function_total_received_bytes</th>
        <th>query_load_percent</th>
        <th>is_client_generated_statement</th>
        <th>query_acceleration_bytes_scanned</th>
        <th>query_acceleration_partitions_scanned</th>
        <th>query_acceleration_upper_limit_scale_factor</th>
        <th>transaction_id</th>
        <th>child_queries_wait_time</th>
        <th>role_type</th>
        <th>query_hash</th>
        <th>query_hash_version</th>
        <th>secondary_role_stats</th>
        <th>rows_written_to_result</th>
        <th>query_retry_time</th>
        <th>query_retry_cause</th>
        <th>fault_handling_time</th>  
        <th>query_text</th>  
        </tr>  
        """
        
        if int(cur.rowcount)!=0:
            for (  RDATE, QUERY_PARAMETERIZED_HASH, TOTAL_ELAPSED_TIME_SEG, GROWTH, QUERY_ID, DATABASE_ID, DATABASE_NAME, SCHEMA_ID, SCHEMA_NAME, QUERY_TYPE, SESSION_ID, USER_NAME, ROLE_NAME, WAREHOUSE_ID, WAREHOUSE_NAME, WAREHOUSE_SIZE, WAREHOUSE_TYPE, CLUSTER_NUMBER, QUERY_TAG, EXECUTION_STATUS, ERROR_CODE, ERROR_MESSAGE, BYTES_SCANNED, PERCENTAGE_SCANNED_FROM_CACHE, BYTES_WRITTEN, BYTES_WRITTEN_TO_RESULT, BYTES_READ_FROM_RESULT, ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_UNLOADED, BYTES_DELETED, PARTITIONS_SCANNED, PARTITIONS_TOTAL, BYTES_SPILLED_TO_LOCAL_STORAGE, BYTES_SPILLED_TO_REMOTE_STORAGE, BYTES_SENT_OVER_THE_NETWORK, COMPILATION_TIME, EXECUTION_TIME, QUEUED_PROVISIONING_TIME, QUEUED_REPAIR_TIME, QUEUED_OVERLOAD_TIME, TRANSACTION_BLOCKED_TIME, OUTBOUND_DATA_TRANSFER_CLOUD, OUTBOUND_DATA_TRANSFER_REGION, OUTBOUND_DATA_TRANSFER_BYTES, INBOUND_DATA_TRANSFER_CLOUD, INBOUND_DATA_TRANSFER_REGION, INBOUND_DATA_TRANSFER_BYTES, LIST_EXTERNAL_FILES_TIME, CREDITS_USED_CLOUD_SERVICES, EXTERNAL_FUNCTION_TOTAL_INVOCATIONS, EXTERNAL_FUNCTION_TOTAL_SENT_ROWS, EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS, EXTERNAL_FUNCTION_TOTAL_SENT_BYTES, EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES, QUERY_LOAD_PERCENT, IS_CLIENT_GENERATED_STATEMENT, QUERY_ACCELERATION_BYTES_SCANNED, QUERY_ACCELERATION_PARTITIONS_SCANNED, QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR, TRANSACTION_ID, CHILD_QUERIES_WAIT_TIME, ROLE_TYPE, QUERY_HASH, QUERY_HASH_VERSION, SECONDARY_ROLE_STATS, ROWS_WRITTEN_TO_RESULT, QUERY_RETRY_TIME, QUERY_RETRY_CAUSE, FAULT_HANDLING_TIME, QUERY_TEXT ) in cur:
                
                html_file=html_file+""" <tr> 
                 <td>"""+str(RDATE)+"""</td> 
                 <td>"""+str(QUERY_PARAMETERIZED_HASH)+"""</td> 
                 <td>"""+str(TOTAL_ELAPSED_TIME_SEG)+"""</td> 
                 <td>"""+str(GROWTH)+"""</td> 
                 <td>"""+str(QUERY_ID)+"""</td> """

                hash_plan=hash_plans.get(str(QUERY_ID),'Execution plan expired')
                if hash_plan=='Execution plan expired':
                    html_file=html_file+"""<td>Execution plan expired</td> """
                else:
                    html_file=html_file+"""<td><a href="./execution_plan_"""+str(QUERY_ID)+"""_"""+str(hash_plan)+""".html">"""+str(hash_plan)+"""</td> """

                html_file=html_file+"""<td>"""+str(DATABASE_ID)+"""</td> 
                 <td>"""+str(DATABASE_NAME)+"""</td> 
                 <td>"""+str(SCHEMA_ID)+"""</td> 
                 <td>"""+str(SCHEMA_NAME)+"""</td> 
                 <td>"""+str(QUERY_TYPE)+"""</td> 
                 <td>"""+str(SESSION_ID)+"""</td> 
                 <td>"""+str(USER_NAME)+"""</td> 
                 <td>"""+str(ROLE_NAME)+"""</td> 
                 <td>"""+str(WAREHOUSE_ID)+"""</td> 
                 <td>"""+str(WAREHOUSE_NAME)+"""</td> 
                 <td>"""+str(WAREHOUSE_SIZE)+"""</td> 
                 <td>"""+str(WAREHOUSE_TYPE)+"""</td> 
                 <td>"""+str(CLUSTER_NUMBER)+"""</td> 
                 <td>"""+str(QUERY_TAG)+"""</td> 
                 <td>"""+str(EXECUTION_STATUS)+"""</td> 
                 <td>"""+str(ERROR_CODE)+"""</td> 
                 <td>"""+str(ERROR_MESSAGE)+"""</td> 
                 <td>"""+str(BYTES_SCANNED)+"""</td> 
                 <td>"""+str(PERCENTAGE_SCANNED_FROM_CACHE)+"""</td> 
                 <td>"""+str(BYTES_WRITTEN)+"""</td> 
                 <td>"""+str(BYTES_WRITTEN_TO_RESULT)+"""</td>  
                 <td>"""+str(BYTES_READ_FROM_RESULT)+"""</td> 
                 <td>"""+str(ROWS_PRODUCED)+"""</td> 
                 <td>"""+str(ROWS_INSERTED)+"""</td> 
                 <td>"""+str(ROWS_UPDATED)+"""</td> 
                 <td>"""+str(ROWS_DELETED)+"""</td> 
                 <td>"""+str(ROWS_UNLOADED)+"""</td> 
                 <td>"""+str(BYTES_DELETED)+"""</td> 
                 <td>"""+str(PARTITIONS_SCANNED)+"""</td> 
                 <td>"""+str(PARTITIONS_TOTAL)+"""</td> 
                 <td>"""+str(BYTES_SPILLED_TO_LOCAL_STORAGE)+"""</td> 
                 <td>"""+str(BYTES_SPILLED_TO_REMOTE_STORAGE)+"""</td> 
                 <td>"""+str(BYTES_SENT_OVER_THE_NETWORK)+"""</td> 
                 <td>"""+str(COMPILATION_TIME)+"""</td> 
                 <td>"""+str(EXECUTION_TIME)+"""</td> 
                 <td>"""+str(QUEUED_PROVISIONING_TIME)+"""</td> 
                 <td>"""+str(QUEUED_REPAIR_TIME)+"""</td> 
                 <td>"""+str(QUEUED_OVERLOAD_TIME)+"""</td> 
                 <td>"""+str(TRANSACTION_BLOCKED_TIME)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_CLOUD)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_REGION)+"""</td> 
                 <td>"""+str(OUTBOUND_DATA_TRANSFER_BYTES)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_CLOUD)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_REGION)+"""</td> 
                 <td>"""+str(INBOUND_DATA_TRANSFER_BYTES)+"""</td> 
                 <td>"""+str(LIST_EXTERNAL_FILES_TIME)+"""</td> 
                 <td>"""+str(CREDITS_USED_CLOUD_SERVICES)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_INVOCATIONS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_SENT_ROWS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_SENT_BYTES)+"""</td> 
                 <td>"""+str(EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES)+"""</td> 
                 <td>"""+str(QUERY_LOAD_PERCENT)+"""</td> 
                 <td>"""+str(IS_CLIENT_GENERATED_STATEMENT)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_BYTES_SCANNED)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_PARTITIONS_SCANNED)+"""</td> 
                 <td>"""+str(QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR)+"""</td> 
                 <td>"""+str(TRANSACTION_ID)+"""</td> 
                 <td>"""+str(CHILD_QUERIES_WAIT_TIME)+"""</td> 
                 <td>"""+str(ROLE_TYPE)+"""</td> 
                 <td>"""+str(QUERY_HASH)+"""</td> 
                 <td>"""+str(QUERY_HASH_VERSION)+"""</td> 
                 <td>"""+str(SECONDARY_ROLE_STATS)+"""</td>  
                 <td>"""+str(ROWS_WRITTEN_TO_RESULT)+"""</td> 
                 <td>"""+str(QUERY_RETRY_TIME)+"""</td> 
                 <td>"""+str(QUERY_RETRY_CAUSE)+"""</td> 
                 <td>"""+str(FAULT_HANDLING_TIME)+"""</td> 
                 <td class="cell_grow">"""+str(QUERY_TEXT)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
    
        create_output_file('last_executions_for_query_'+sql_query_id+'.html',html_file)
        report_sections["D - Performance"].update({'last_executions_for_query_'+sql_query_id+'.html':'table'})

    except Exception as error:
        print("[table_last_executions_of_query]: An exception occurred:", error)

def line_history_wh_changes_by_query(conn,sql_query_id):
    try:            
        global snowflake_conn
        global report_sections
        conn =snowflake_conn

        sql_query=sql_header+"""
        SELECT 
            DISTINCT WAREHOUSE_NAME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
        WHERE  TO_DATE(Q.START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
        AND QUERY_PARAMETERIZED_HASH='"""+sql_query_id+"""'
        """
        cur = conn.cursor()
        cur.execute(sql_query)

        html_file=html_header
        column_count=cur.rowcount
        if int(cur.rowcount)!=0:
            headers =[i[0] for i in cur]  
            headers.insert(0,'DATE')  					
            html_file=html_file+str(headers).replace('"','')+", \n"

            sql_query=sql_header+"""
            WITH DATA AS (
                SELECT 
                    DATE_TRUNC('MINUTE',START_TIME::TIMESTAMP_NTZ) DATE, 
                    WAREHOUSE_NAME,
                    1   WH_ACTIVE
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
                WHERE  TO_DATE(Q.START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                AND DATABASE_NAME IS NOT NULL
                AND QUERY_PARAMETERIZED_HASH='"""+sql_query_id+"""'
            )
            , PIVOT_DATA AS (
                SELECT *
                FROM DATA
                    PIVOT(  MAX (WH_ACTIVE) FOR WAREHOUSE_NAME IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) ) 
            )
            SELECT ARRAY_CONSTRUCT(*) FROM PIVOT_DATA ORDER BY DATE
            """
            cur = conn.cursor()
            cur.execute(sql_query)

            if int(cur.rowcount)!=0:
                counter=0
                for row  in cur:
                    counter=counter+1
                    if counter==cur.rowcount:
                        html_file=html_file+str(row[0])
                    else:
                        html_file=html_file+str(row[0])+""","""
                        
                html_file=html_file+html_body1
                        
                html_file=html_file+"""row[1]"""
                
            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Warehouse Changes for the query """+sql_query_id+"""`,"""+html_stepped_area_minute_tail

            create_output_file('history_wh_changes_for_'+sql_query_id+'.html',html_file)
            report_sections["D - Performance"].update({'history_wh_changes_for_'+sql_query_id+'.html':'line'})
                
    except Exception as error:
        print("[line_history_wh_changes_by_query]: An exception occurred:", error)

def line_history_size_changes_by_warehouse(conn,warehouse_name):
    try:            
        global snowflake_conn
        global report_sections
        conn =snowflake_conn

        sql_query=sql_header+"""
        SELECT 
            DISTINCT WAREHOUSE_SIZE
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
        WHERE  TO_DATE(Q.START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
        AND WAREHOUSE_NAME='"""+warehouse_name+"""'
        AND WAREHOUSE_SIZE IS NOT NULL
        """
        cur = conn.cursor()
        cur.execute(sql_query)

        html_file=html_header
        column_count=cur.rowcount
        if int(cur.rowcount)!=0:
            headers =[i[0] for i in cur]  
            headers.insert(0,'DATE')  					
            html_file=html_file+str(headers).replace('"','')+", \n"

            sql_query=sql_header+"""
            WITH DATA AS (
                SELECT 
                    DATE_TRUNC('MINUTE',START_TIME::TIMESTAMP_NTZ) DATE, 
                    warehouse_size,
                    1   ACTIVE_SIZE
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
                WHERE  TO_DATE(Q.START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                AND DATABASE_NAME IS NOT NULL
                AND WAREHOUSE_NAME='"""+warehouse_name+"""'
            )
            , PIVOT_DATA AS (
                SELECT *
                FROM DATA
                    PIVOT(  MAX (ACTIVE_SIZE) FOR warehouse_size IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) ) 
            )
            SELECT ARRAY_CONSTRUCT(*) FROM PIVOT_DATA ORDER BY DATE
            """
            cur = conn.cursor()
            cur.execute(sql_query)

            if int(cur.rowcount)!=0:
                counter=0
                for row  in cur:
                    counter=counter+1
                    if counter==cur.rowcount:
                        html_file=html_file+str(row[0])
                    else:
                        html_file=html_file+str(row[0])+""","""
                        
                html_file=html_file+html_body1
                        
                html_file=html_file+"""row[1]"""
                
            html_file=html_file+html_body2+"""
            vAxis: {  title: 'Active', titleTextStyle: {fontSize: 14},viewWindowMode:'explicit', viewWindow: {max:2,min:0}},
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Size changes for warehouse """+warehouse_name+"""`,"""+html_stepped_area_minute_tail

            create_output_file('history_size_changes_for_wh_'+warehouse_name.lower()+'.html',html_file)
            report_sections["A - Computing"].update({'history_size_changes_for_wh_'+warehouse_name.lower()+'.html':'line'})
                
    except Exception as error:
        print("[line_history_size_changes_by_warehouse]: An exception occurred:", error)

def table_history_accessed_objects_by_query(conn,sql_query_id):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail     
        global report_sections   
        conn =snowflake_conn
        
        sql_query=sql_header+"""
        SELECT
            DISTINCT 
            query_id
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
        WHERE query_parameterized_hash='"""+sql_query_id+"""'
        AND  TO_DATE(START_TIME) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
        """

        cur_q = conn.cursor()
        cur_q.execute(sql_query)

        html_file=html_table_header     
        html_file=html_file+"""
        <h3>Accessed Objects for Query """+sql_query_id+"""</h3>
        <table class="tabla2">
        <tr>
        <th >QUERY_PARAMETERIZED_HASH</th>
        <th >DATABASE_NAME</th>
        <th >SCHEMA_NAME</th>
        <th >OBJECT_NAME</th>
        <th >OBJECT_TYPE</th>
        <th >ACTIVE_GB</th>
        <th >TIME_TRAVEL_BYTES</th>
        <th >FAILSAFE_BYTES</th>
        """      
        if int(cur_q.rowcount)!=0:
            list_query_id =[i[0] for i in cur_q]  
        
            sql_query=sql_header+"""
            WITH HIST_DATA AS (
                SELECT
                    T.query_id  AS QUERY_ID,
                    R.VALUE:"objectName" AS OBJECT_NAME,
                    R.VALUE:"objectDomain" AS OBJECT_TYPE,
                FROM
                    (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY) T,
                    LATERAL FLATTEN(INPUT => T.BASE_OBJECTS_ACCESSED) R
                WHERE query_id IN ( """+str(list_query_id).replace('"','').replace("[","").replace("]","")+""") 
                AND  TO_DATE(query_start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                UNION ALL
                SELECT
                    T.query_id  AS QUERY_ID,
                    W.VALUE:"objectName" AS OBJECT_NAME,
                    W.VALUE:"objectDomain" AS OBJECT_TYPE
                FROM
                    (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY) T,
                    LATERAL FLATTEN(INPUT => T.OBJECTS_MODIFIED) W
                WHERE query_id IN ( """+str(list_query_id).replace('"','').replace("[","").replace("]","")+""")
                AND  TO_DATE(query_start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
                UNION ALL
                SELECT
                    T.query_id  AS QUERY_ID,
                    D.VALUE:"objectName" AS OBJECT_NAME,
                    D.VALUE:"objectDomain" AS OBJECT_TYPE
                FROM
                    (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY) T,
                    LATERAL FLATTEN(INPUT => T.direct_objects_accessed) D
                WHERE query_id IN ( """+str(list_query_id).replace('"','').replace("[","").replace("]","")+""")
                AND  TO_DATE(query_start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            )
            , DATA AS (
            SELECT DISTINCT
                STRTOK(OBJECT_NAME, '.', 1)     AS DATABASE_NAME, 
                STRTOK(OBJECT_NAME, '.', 2)     AS SCHEMA_NAME, 
                STRTOK(OBJECT_NAME, '.', 3)     AS OBJECT_NAME, 
                REPLACE(OBJECT_TYPE,'"')        AS OBJECT_TYPE
            FROM HIST_DATA 
            )
            SELECT 
                DATABASE_NAME,
                SCHEMA_NAME,
                OBJECT_NAME,
                OBJECT_TYPE,
                ROUND(ACTIVE_BYTES/1024/1024/1024,2) ACTIVE_GB,
                ROUND(TIME_TRAVEL_BYTES/1024/1024/1024,2)  TIME_TRAVEL_BYTES,
                ROUND(FAILSAFE_BYTES/1024/1024/1024,2)  FAILSAFE_BYTES
            FROM DATA D LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS S
            ON (
                D.DATABASE_NAME=S.TABLE_CATALOG
                AND D.SCHEMA_NAME=TABLE_SCHEMA
                AND D.OBJECT_NAME=TABLE_NAME
            )
            WHERE S.DELETED=FALSE
            ORDER BY 5 DESC,1,2,3
            ;
            """
            cur = conn.cursor()
            cur.execute(sql_query)
        
            if int(cur.rowcount)!=0:
                for ( ROW_DATABASE_NAME,ROW_SCHEMA_NAME, ROW_OBJECT_NAME, ROW_OBJECT_TYPE, ROW_ACTIVE_GB, ROW_TIME_TRAVEL_GB, ROW_FAILSAFE_GB) in cur:
                    if str(ROW_OBJECT_TYPE)=='Table':
                        line_history_pruning_efficiency_by_table(conn,ROW_DATABASE_NAME.lower(),ROW_SCHEMA_NAME.lower(),ROW_OBJECT_NAME.lower())
                    html_file=html_file+""" <tr> 
                    <td>"""+str(sql_query_id)+"""</td> 
                    <td>"""+str(ROW_DATABASE_NAME)+"""</td> 
                    <td>"""+str(ROW_SCHEMA_NAME)+"""</td> 
                    <td>"""+str(ROW_OBJECT_NAME)+"""</td> 
                    <td>"""+str(ROW_ACTIVE_GB)+"""</td> 
                    <td>"""+str(ROW_TIME_TRAVEL_GB)+"""</td> 
                    <td>"""+str(ROW_FAILSAFE_GB)+"""</td> 
                    <td class="cell_grow">"""+str(ROW_OBJECT_TYPE)+"""</td> 
                    </tr> """
            
        html_file=html_file+html_table_tail

        create_output_file('accessed_objects_for_query_'+sql_query_id+'.html',html_file)
        report_sections["D - Performance"].update({'accessed_objects_for_query_'+sql_query_id+'.html':'table'})
    
    except Exception as error:
        print("[table_history_accessed_objects_by_query]: An exception occurred:", error)

def table_explain_by_query(conn,query_parameterized_hash):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail     
        global report_sections   
        
        conn =snowflake_conn                  

        sql_query=sql_header+"""
        WITH LIST_QUERY_ID AS (
        SELECT DISTINCT '"""+query_parameterized_hash+"""' query_parameterized_hash,QUERY_ID 
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
        WHERE query_parameterized_hash='"""+query_parameterized_hash+"""'    
        AND START_TIME > DATEADD('DAY',-13,CURRENT_DATE())
        AND QUERY_ID IS NOT NULL
        )
        SELECT 'select '''||query_parameterized_hash||''' query_parameterized_hash,* from table(get_query_operator_stats( '''||QUERY_ID||''')) UNION ALL ' AS DATA FROM LIST_QUERY_ID
        """

        cur = conn.cursor()
        cur.execute(sql_query)
        sql_query=sql_header+'WITH EXECUTION_PLANS AS ('
        counter=0
        for row in cur: 
            counter=counter+1
            if counter==cur.rowcount:
                sql_query=sql_query+row[0].replace("UNION ALL","")
            else:
                sql_query=sql_query+row[0]+"\n"
        sql_query=sql_query+"""
        )
        ,  DATA_WITH_HASH AS (
            SELECT
                hash_agg(query_parameterized_hash,STEP_ID,OPERATOR_ID,PARENT_OPERATORS,OPERATOR_TYPE) OVER (PARTITION BY query_parameterized_hash,QUERY_ID) AS  EXECUTION_PLAN_HASH,
                *
            FROM   EXECUTION_PLANS              
        )
        , UNPIVOTED_DATA AS (
        SELECT 
            EXECUTION_PLAN_HASH,
            QUERY_ID,
            STEP_ID,
            OPERATOR_ID,
            PARENT_OPERATORS,
            OPERATOR_TYPE,
            OPERATOR_ATTRIBUTES,            
            CASE WHEN OSC.key IS NULL THEN OS.KEY ELSE   OSC.KEY END STATISTIC_NAME,
            CASE WHEN OSC.key IS NULL THEN OS.VALUE ELSE OSC.VALUE END  STATISTIC_VALUE,    
            CASE WHEN ETBC.key IS NULL THEN ETB.key ELSE    ETBC.key END TIME_STAT_NAME,
            CASE WHEN ETBC.key IS NULL THEN ETB.value ELSE  ETBC.value END  TIME_STAT_VALUE
        FROM DATA_WITH_HASH EP,
        LATERAL FLATTEN(input => EP.OPERATOR_STATISTICS, OUTER => TRUE) OS, 
        LATERAL FLATTEN(INPUT => OS.value, OUTER => TRUE) OSC,
        LATERAL FLATTEN(input => EP.EXECUTION_TIME_BREAKDOWN, OUTER => TRUE) ETB, 
        LATERAL FLATTEN(INPUT => ETB.value, OUTER => TRUE) ETBC
        ORDER BY  query_parameterized_hash,QUERY_ID,STEP_ID,OPERATOR_ID,    PARENT_OPERATORS
        )
        , PIVOT_STATISTIC AS (
        SELECT * 
        FROM  UNPIVOTED_DATA
        PIVOT(MAX(STATISTIC_VALUE) FOR STATISTIC_NAME IN (SELECT DISTINCT STATISTIC_NAME FROM UNPIVOTED_DATA WHERE STATISTIC_NAME IS NOT NULL )  DEFAULT ON NULL (0) ),
        )
        , PIVOT_TIME_STAT AS (
        SELECT * 
        FROM PIVOT_STATISTIC
        PIVOT(MAX(TIME_STAT_VALUE) FOR TIME_STAT_NAME IN (SELECT DISTINCT TIME_STAT_NAME FROM PIVOT_STATISTIC WHERE TIME_STAT_NAME IS NOT NULL)  DEFAULT ON NULL (0) ),
        )
        , DATA AS (
        SELECT * FROM PIVOT_TIME_STAT
        )
        SELECT * FROM DATA 
        ORDER BY EXECUTION_PLAN_HASH,QUERY_ID,STEP_ID,OPERATOR_ID,    PARENT_OPERATORS
        ;
        """

        cur = conn.cursor()
        cur.execute(sql_query)
        current_query=''
        execution_plan_hash=''
        if int(cur.rowcount)!=0:

            reset_bit=0
            
            for row in cur:    

                if current_query!=str(row[1]):
                    if current_query!='':
                        html_file=html_file+"""</tr> """            
                        html_file=html_file+html_table_tail
                        create_output_file('execution_plan_'+current_query+'_'+execution_plan_hash+'.html',html_file)
                        hash_plans[current_query]= execution_plan_hash
                    current_query=str(row[1])
                    execution_plan_hash=str(row[0])
                    html_file=html_table_header      
                    column_count=len(cur.description)
                    headers =[] 
                    for i in cur.description:
                        headers.append(i[0])
                    html_file=html_file+"""
                    <table class="tabla1">
                    """+str(headers).upper().replace('"','').replace(",","</th><th>").replace("[","<tr><th>").replace("]","</th></tr>")
                

                    html_file=html_file+"""<tr>"""
                      

                for i in range(0, column_count):
                    if i==column_count-1:
                        html_file=html_file+"""<td class="cell_grow">"""+str(row[i]).replace("None","")+"""</td> """                        
                    else:

                        if reset_bit==1:
                            html_file=html_file+"""</td> """
                            reset_bit=0
                        html_file=html_file+"""<td>"""+str(row[i]).replace("None","")+"""</td> """


                html_file=html_file+"""</tr> """            
            html_file=html_file+html_table_tail
            create_output_file('execution_plan_'+row[1]+'_'+execution_plan_hash+'.html',html_file)
            hash_plans[row[1]]= execution_plan_hash

    except Exception as error:
        print (error)

def line_history_storage_stages(conn):
    try:
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
        
        html_file=html_header+"""
            ['DATE','AVERAGE_STAGE_GB'],
        """
        
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT 
                DATE_TRUNC('DAY',USAGE_DATE::TIMESTAMP_NTZ)         AS DATE, 
                ROUND(SUM(AVERAGE_STAGE_BYTES/1024/1024/1024),2)    AS AVERAGE_STAGE_GB
            FROM  snowflake.account_usage.STAGE_STORAGE_USAGE_HISTORY  
            WHERE
                TO_DATE(USAGE_DATE) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            GROUP BY 1
            ORDER BY 1 
        )
        SELECT ARRAY_CONSTRUCT(*) AS DATA FROM DATA;
        """
        cur_details = conn.cursor()
        cur_details.execute(sql_query)
    
        if int(cur_details.rowcount)!=0:
            counter=0
            for (row_data) in cur_details:
                counter=counter+1
                if counter==cur_details.rowcount:
                    html_file=html_file+str(row_data[0])
                else:
                    html_file=html_file+str(row_data[0])+""","""
    
            html_file=html_file+html_body1+"""
            row[1]
            """+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Storage Usage for Stages`,"""+html_line_hour_tail
                
            create_output_file('storage_stages.html',html_file)
            report_sections["B - Storage"].update({'storage_stages.html':'line'})
            
    except Exception as error:
        print("[line_history_storage_stages]: An exception occurred:", error)

def line_history_bytes_replication_by_database(conn):
    try:
        
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
            
        html_file=html_header

        sql_query=sql_header+"""
        SELECT 
            DISTINCT DATABASE_NAME
        FROM snowflake.account_usage.DATABASE_REPLICATION_USAGE_HISTORY  Q
        WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
        AND DATABASE_NAME !='SNOWFLAKE'
        """
        cur = conn.cursor()
        cur.execute(sql_query)

        column_count=cur.rowcount        
        headers =[i[0] for i in cur]  
        headers.insert(0,'DATE')  
        html_file=html_file+str(headers).replace('"','')+", \n"      


        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('HOUR',START_TIME::TIMESTAMP_NTZ)         AS DATE , 
            DATABASE_NAME                                               AS DATABASE_NAME,
            ROUND(SUM(BYTES_TRANSFERRED/1024/1024/1024),2)              AS GB_TRANSFERRED
            FROM snowflake.account_usage.DATABASE_REPLICATION_USAGE_HISTORY  Q
            WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND DATABASE_NAME IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""")            
            GROUP BY 1,2
            ORDER BY 1  
        )
        , PIVOT_DATA AS (
            SELECT *
            FROM DATA
                PIVOT(  sum (GB_TRANSFERRED) FOR DATABASE_NAME IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) ) 
        )
        SELECT ARRAY_CONSTRUCT(*) FROM PIVOT_DATA ORDER BY DATE
        """

        cur = conn.cursor()
        cur.execute(sql_query)
        
        if int(cur.rowcount)!=0:
            for row  in cur:
                html_file=html_file+str(row[0])+""","""
            
            
            html_file=html_file+html_body1
                
            for i in range(1, column_count):
                if i==column_count-1:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""
            
            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Replication usage in bytes by database`,"""+html_line_hour_tail

            create_output_file('bytes_replication_by_database.html',html_file)
            report_sections["F - Data Transfer"].update({'bytes_replication_by_database.html':'line'})
        
    except Exception as error:
        print("[line_history_bytes_replication_by_database]: An exception occurred:", error)

def line_history_credits_replication_by_database(conn):
    try:
        
        global snowflake_conn
        global report_sections
        conn =snowflake_conn
            
        html_file=html_header

        sql_query=sql_header+"""
        SELECT 
            DISTINCT DATABASE_NAME
        FROM snowflake.account_usage.DATABASE_REPLICATION_USAGE_HISTORY  Q
        WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
        AND DATABASE_NAME !='SNOWFLAKE'
        """
        cur = conn.cursor()
        cur.execute(sql_query)

        column_count=cur.rowcount        
        headers =[i[0] for i in cur]  
        headers.insert(0,'DATE')  
        html_file=html_file+str(headers).replace('"','')+", \n"      


        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT DATE_TRUNC('HOUR',START_TIME::TIMESTAMP_NTZ)         AS DATE , 
            DATABASE_NAME                                               AS DATABASE_NAME,
            ROUND(SUM(CREDITS_USED),2)                                  AS CREDITS_USED
            FROM snowflake.account_usage.DATABASE_REPLICATION_USAGE_HISTORY  Q
            WHERE  TO_DATE(Q.start_time) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP("""+report_formatted_time+"""))
            AND DATABASE_NAME IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""")            
            GROUP BY 1,2
            ORDER BY 1  
        )
        , PIVOT_DATA AS (
            SELECT *
            FROM DATA
                PIVOT(  sum (CREDITS_USED) FOR DATABASE_NAME IN ( """+str(headers).replace('"','').replace("[","").replace("]","").replace("'DATE',","")+""") DEFAULT ON NULL (0) ) 
        )
        SELECT ARRAY_CONSTRUCT(*) FROM PIVOT_DATA ORDER BY DATE
        """

        cur = conn.cursor()
        cur.execute(sql_query)
        
        if int(cur.rowcount)!=0:
            for row  in cur:
                html_file=html_file+str(row[0])+""","""
                        
            html_file=html_file+html_body1
                
            for i in range(1, column_count):
                if i==column_count-1:
                    html_file=html_file+"""row["""+str(i)+"""]"""
                else:
                    html_file=html_file+"""row["""+str(i)+"""],"""
            
            html_file=html_file+html_body2+"""
            title: `Prismafy v1.0 - https://github.com/prismafy/prismafy
            Chart Creation Date: """+report_formatted_time+"""
            Credits Used for Replication by database`,"""+html_line_hour_tail

            create_output_file('credits_replication_by_database.html',html_file)
            report_sections["C - Credits"].update({'credits_replication_by_database.html':'line'})
        
    except Exception as error:
        print("[line_history_credits_replication_by_database]: An exception occurred:", error)

def table_history_client_driver_changes(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn        
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT                 
                DATE_TRUNC('HOUR',EVENT_TIMESTAMP::TIMESTAMP_NTZ)   AS DATE, 
                USER_NAME                                           AS USER_NAME ,
                CLIENT_IP                                           AS CLIENT_IP,
                REPORTED_CLIENT_TYPE                                AS REPORTED_CLIENT_TYPE,
                REPORTED_CLIENT_VERSION                             AS REPORTED_CLIENT_VERSION,
                FIRST_AUTHENTICATION_FACTOR                         AS FIRST_AUTHENTICATION_FACTOR,
                SECOND_AUTHENTICATION_FACTOR                        AS SECOND_AUTHENTICATION_FACTOR,
                ERROR_CODE                                          AS ERROR_CODE,
                ERROR_MESSAGE                                       AS ERROR_MESSAGE
            FROM snowflake.account_usage.LOGIN_HISTORY   Q
            WHERE  TO_DATE(EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP(CURRENT_DATE()))
            QUALIFY   REPORTED_CLIENT_TYPE!=
            LAG (REPORTED_CLIENT_TYPE,1,NULL)  OVER (PARTITION BY USER_NAME ORDER BY EVENT_TIMESTAMP)
        )
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY USER_NAME ORDER BY DATE DESC)       AS row_num,
            *
        FROM DATA
        QUALIFY row_num<=50 
        ORDER BY USER_NAME,1
        LIMIT 1000
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Users with changes on client driver used for login</h3>
        <table class="tabla1">
        <tr>
        <th >Num</th>
        <th >DATE</th>
        <th >USER_NAME</th>
        <th >CLIENT_IP</th>
        <th >REPORTED_CLIENT_TYPE</th>
        <th >REPORTED_CLIENT_VERSION</th>
        <th >FIRST_AUTHENTICATION_FACTOR</th>
        <th >SECOND_AUTHENTICATION_FACTOR</th>
        <th >ERROR_CODE</th>
        <th >ERROR_MESSAGE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_NUM,ROW_DATE, ROW_COUNT_LOGINS, ROW_CLIENT_IP, ROW_REPORTED_CLIENT_TYPE, ROW_REPORTED_CLIENT_VERSION, ROW_FIRST_AUTHENTICATION_FACTOR, ROW_SECOND_AUTHENTICATION_FACTOR, ROW_ERROR_CODE, ROW_ERROR_MESSAGE) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_NUM)+"""</td>
                <td>"""+str(ROW_DATE)+"""</td> 
                <td>"""+str(ROW_COUNT_LOGINS)+"""</td> 
                 <td>"""+str(ROW_CLIENT_IP)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_TYPE)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_VERSION)+"""</td> 
                 <td>"""+str(ROW_FIRST_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_SECOND_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_ERROR_CODE)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_ERROR_MESSAGE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
        
        create_output_file('changes_on_client_driver_used_for_logins.html',html_file)
        report_sections["E - Security"].update({'changes_on_client_driver_used_for_logins.html':'table'})
    
    except Exception as error:
        print("[table_history_client_driver_changes]: An exception occurred:", error)

def table_history_ip_changes(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn        
        html_file=html_table_header
        
        sql_query=sql_header+"""
        WITH DATA AS (
            SELECT
                DATE_TRUNC('HOUR',EVENT_TIMESTAMP::TIMESTAMP_NTZ)   AS DATE, 
                USER_NAME                                           AS USER_NAME ,
                CLIENT_IP                                           AS CLIENT_IP,
                REPORTED_CLIENT_TYPE                                AS REPORTED_CLIENT_TYPE,
                REPORTED_CLIENT_VERSION                             AS REPORTED_CLIENT_VERSION,
                FIRST_AUTHENTICATION_FACTOR                         AS FIRST_AUTHENTICATION_FACTOR,
                SECOND_AUTHENTICATION_FACTOR                        AS SECOND_AUTHENTICATION_FACTOR,
                ERROR_CODE                                          AS ERROR_CODE,
                ERROR_MESSAGE                                       AS ERROR_MESSAGE
            FROM snowflake.account_usage.LOGIN_HISTORY   Q
            WHERE  TO_DATE(EVENT_TIMESTAMP) > DATEADD(MONTH,"""+months_history+""",TO_TIMESTAMP(CURRENT_DATE()))
            QUALIFY   CLIENT_IP!=
            LAG (CLIENT_IP,1,NULL)  OVER (PARTITION BY USER_NAME ORDER BY EVENT_TIMESTAMP)            
        )
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY USER_NAME ORDER BY DATE DESC)       AS row_num,
            *
        FROM DATA
        QUALIFY row_num<=50 
        ORDER BY USER_NAME,1
        LIMIT 1000
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Users with changes on IP used for login</h3>
        <table class="tabla1">
        <tr>
        <th >Num</th>
        <th >DATE</th>
        <th >USER_NAME</th>
        <th >CLIENT_IP</th>
        <th >REPORTED_CLIENT_TYPE</th>
        <th >REPORTED_CLIENT_VERSION</th>
        <th >FIRST_AUTHENTICATION_FACTOR</th>
        <th >SECOND_AUTHENTICATION_FACTOR</th>
        <th >ERROR_CODE</th>
        <th >ERROR_MESSAGE</th>
        """
    
        if int(cur.rowcount)!=0:
            for (ROW_NUM,ROW_DATE, ROW_COUNT_LOGINS, ROW_CLIENT_IP, ROW_REPORTED_CLIENT_TYPE, ROW_REPORTED_CLIENT_VERSION, ROW_FIRST_AUTHENTICATION_FACTOR, ROW_SECOND_AUTHENTICATION_FACTOR, ROW_ERROR_CODE, ROW_ERROR_MESSAGE) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(ROW_NUM)+"""</td> 
                <td>"""+str(ROW_DATE)+"""</td> 
                <td>"""+str(ROW_COUNT_LOGINS)+"""</td> 
                 <td>"""+str(ROW_CLIENT_IP)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_TYPE)+"""</td> 
                 <td>"""+str(ROW_REPORTED_CLIENT_VERSION)+"""</td> 
                 <td>"""+str(ROW_FIRST_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_SECOND_AUTHENTICATION_FACTOR)+"""</td> 
                 <td>"""+str(ROW_ERROR_CODE)+"""</td> 
                 <td class="cell_grow">"""+str(ROW_ERROR_MESSAGE)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
        
        create_output_file('changes_on_ip_used_for_logins.html',html_file)
        report_sections["E - Security"].update({'changes_on_ip_used_for_logins.html':'table'})

    
    except Exception as error:
        print("[table_history_ip_changes]: An exception occurred:", error)

def table_history_external_functions(conn):

    try:    
        global snowflake_conn
        global html_table_header
        global html_table_tail
        global report_sections
        conn =snowflake_conn        
        html_file=html_table_header
        
        sql_query=sql_header+"""
        SELECT                 
            FUNCTION_NAME, 
            FUNCTION_SCHEMA,
            FUNCTION_CATALOG FUNCTION_DATABASE,
            FUNCTION_OWNER,
            CREATED,
            LAST_ALTERED,
            API_INTEGRATION ,
            EXTERNAL_ACCESS_INTEGRATIONS,
            DATA_TYPE,
            ARGUMENT_SIGNATURE,
            CHARACTER_MAXIMUM_LENGTH,
            CHARACTER_OCTET_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_PRECISION_RADIX,
            NUMERIC_SCALE,
            FUNCTION_LANGUAGE,                
            VOLATILITY,
            IS_NULL_CALL,
            CONTEXT_HEADERS ,
            MAX_BATCH_ROWS ,
            COMPRESSION ,
            PACKAGES,
            RUNTIME_VERSION,
            INSTALLED_PACKAGES,
            OWNER_ROLE_TYPE,
            IS_MEMOIZABLE,
            IS_DATA_METRIC,
            FUNCTION_DEFINITION
        FROM snowflake.account_usage.FUNCTIONS 
        WHERE DELETED IS NULL 
        AND IS_EXTERNAL ='YES'
        ORDER BY CREATED DESC
        """
        
        cur = conn.cursor()
        cur.execute(sql_query)
    
        html_file=html_file+"""
        <h3>Users with changes on client driver used for login</h3>
        <table class="tabla1">
        <tr>
        <th >FUNCTION_NAME</th>
        <th >FUNCTION_SCHEMA</th>
        <th >FUNCTION_DATABASE</th>
        <th >FUNCTION_OWNER</th>
        <th >CREATED</th>
        <th >LAST_ALTERED</th>
        <th >API_INTEGRATION </th>
        <th >EXTERNAL_ACCESS_INTEGRATIONS</th>
        <th >DATA_TYPE</th>
        <th >ARGUMENT_SIGNATURE</th>
        <th >CHARACTER_MAXIMUM_LENGTH</th>
        <th >CHARACTER_OCTET_LENGTH</th>
        <th >NUMERIC_PRECISION</th>
        <th >NUMERIC_PRECISION_RADIX</th>
        <th >NUMERIC_SCALE</th>
        <th >FUNCTION_LANGUAGE</th>
        <th >VOLATILITY</th>
        <th >IS_NULL_CALL</th>
        <th >CONTEXT_HEADERS </th>
        <th >MAX_BATCH_ROWS </th>
        <th >COMPRESSION </th>
        <th >PACKAGES</th>
        <th >RUNTIME_VERSION</th>
        <th >INSTALLED_PACKAGES</th>
        <th >OWNER_ROLE_TYPE</th>
        <th >IS_MEMOIZABLE</th>
        <th >IS_DATA_METRIC</th>
        <th >FUNCTION_DEFINITION</th>
        """
    
        if int(cur.rowcount)!=0:
            for (FUNCTION_NAME, FUNCTION_SCHEMA, FUNCTION_DATABASE, FUNCTION_OWNER, CREATED, LAST_ALTERED, API_INTEGRATION , EXTERNAL_ACCESS_INTEGRATIONS, DATA_TYPE, ARGUMENT_SIGNATURE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE, FUNCTION_LANGUAGE, VOLATILITY, IS_NULL_CALL, CONTEXT_HEADERS , MAX_BATCH_ROWS , COMPRESSION , PACKAGES, RUNTIME_VERSION, INSTALLED_PACKAGES, OWNER_ROLE_TYPE, IS_MEMOIZABLE, IS_DATA_METRIC, FUNCTION_DEFINITION) in cur:
                html_file=html_file+""" <tr> 
                <td>"""+str(FUNCTION_NAME)+"""</td>
                <td>"""+str(FUNCTION_SCHEMA)+"""</td> 
                <td>"""+str(FUNCTION_DATABASE)+"""</td> 
                <td>"""+str(FUNCTION_OWNER)+"""</td> 
                <td>"""+str(CREATED)+"""</td> 
                 <td>"""+str(LAST_ALTERED)+"""</td> 
                 <td>"""+str(API_INTEGRATION)+"""</td> 
                 <td>"""+str(EXTERNAL_ACCESS_INTEGRATIONS)+"""</td> 
                 <td>"""+str(DATA_TYPE)+"""</td> 
                 <td>"""+str(ARGUMENT_SIGNATURE)+"""</td> 
                 <td>"""+str(CHARACTER_MAXIMUM_LENGTH)+"""</td> 
                 <td>"""+str(CHARACTER_OCTET_LENGTH)+"""</td> 
                 <td>"""+str(NUMERIC_PRECISION)+"""</td> 
                 <td>"""+str(NUMERIC_PRECISION_RADIX)+"""</td> 
                 <td>"""+str(NUMERIC_SCALE)+"""</td> 
                 <td>"""+str(FUNCTION_LANGUAGE)+"""</td> 
                 <td>"""+str(VOLATILITY)+"""</td> 
                 <td>"""+str(IS_NULL_CALL)+"""</td> 
                 <td>"""+str(CONTEXT_HEADERS)+"""</td> 
                 <td>"""+str(MAX_BATCH_ROWS)+"""</td> 
                 <td>"""+str(COMPRESSION)+"""</td> 
                 <td>"""+str(PACKAGES)+"""</td> 
                 <td>"""+str(RUNTIME_VERSION)+"""</td> 
                 <td>"""+str(INSTALLED_PACKAGES)+"""</td> 
                 <td>"""+str(OWNER_ROLE_TYPE)+"""</td> 
                 <td>"""+str(IS_MEMOIZABLE)+"""</td> 
                 <td>"""+str(IS_DATA_METRIC)+"""</td> 
                 <td class="cell_grow">"""+str(FUNCTION_DEFINITION)+"""</td> 
                </tr> """
            
            html_file=html_file+html_table_tail
        
        create_output_file('external_functions.html',html_file)
        report_sections["F - Data Transfer"].update({'external_functions.html':'table'})
    
    except Exception as error:
        print("[table_history_external_functions]: An exception occurred:", error)

def report_builder():
    global report_sections
    global html_table_tail
    global html_table_header_index
    move_icon()
    report_html_index=html_table_header_index   

    change_table=0
    for section, report_list in report_sections.items():
        
        if str(section)=='D - Performance' or str(section)=='E - Security':
            report_html_index=report_html_index+"""</div><div class="column">"""
        if change_table==0:
            report_html_index=report_html_index+"""
            <h2>Section """+str(section)+"""</h2>
            <table class="tabla1">
            <th >Type</th>
            <th >HTML File Name</th>
            """    
            change_table=1
        else:
            report_html_index=report_html_index+"""            
            <h2>Section """+str(section)+"""</h2>            
            <table class="tabla2">
            <th >Type</th>
            <th >HTML File Name</th>
            """ 
            change_table=0

        for html_page_name, html_page_type in report_list.items():
            report_html_index=report_html_index+""" <tr> 
            <td><a href="./"""+str(html_page_name)+"""">"""+str(html_page_type)+"""</td> 
            <td>"""+str(html_page_name)+"""</td> 
            </tr> """
        report_html_index=report_html_index+""" </table> """


    report_html_index=report_html_index+"""</div></div>"""
    report_html_index=report_html_index+"""</body> </html>"""
    create_output_file('prismafy_index.html',report_html_index)

def sections_builder():
    print ("Copyright (C) 2024 - prismafy\n ")

    if args.analyzequery is not None and args.analyzewarehouse is not None:
        print ("Arguments 'analyzequery' and 'analyzewarehouse' cannot be set at the same time. ")
        return
    
    global snowflake_conn 
    snowflake_conn=create_snowflake_db_connection(args.authenticator)

    if snowflake_conn==-1:
        return -1    

    report_start_time = datetime.now()

    if args.analyzequery is not None:
        print("Working on report for query "+args.analyzequery.lower())
        section_start_time = datetime.now()
        line_history_bytes_details_by_query_parameterized_hash(snowflake_conn,args.analyzequery.lower())
        line_history_calls_details_by_query_parameterized_hash(snowflake_conn,args.analyzequery.lower())
        line_history_time_details_by_query_parameterized_hash(snowflake_conn,args.analyzequery.lower())
        line_history_rows_details_by_query_parameterized_hash(snowflake_conn,args.analyzequery.lower())
        table_last_executions_of_query(snowflake_conn,args.analyzequery.lower())
        line_history_wh_changes_by_query(snowflake_conn,args.analyzequery.lower())
        table_history_accessed_objects_by_query(snowflake_conn,args.analyzequery.lower())
        print ("Query Analysis Duration: "+ str(round(  (datetime.now()- section_start_time).total_seconds()/60 ,2) )+" minutes." )

    elif args.analyzewarehouse is not None:
        print("Working on report for warehouse "+args.analyzewarehouse.upper())
        section_start_time = datetime.now()
        line_history_load_details_by_warehouse(snowflake_conn,args.analyzewarehouse.upper() )
        bar_month_load_details_by_warehouse(snowflake_conn,args.analyzewarehouse.upper() )
        bar_week_load_details_by_warehouse(snowflake_conn,args.analyzewarehouse.upper() )
        table_history_warehouse_events(snowflake_conn,args.analyzewarehouse.upper() )
        line_history_warehouse_enable_vs_querycount(snowflake_conn,args.analyzewarehouse.upper() )
        line_history_size_changes_by_warehouse(snowflake_conn,args.analyzewarehouse.upper() )
        print ("Warehouse Analysis Duration: "+ str(round(  (datetime.now()- section_start_time).total_seconds()/60 ,2) )+" minutes." )
    else:   
        if args.reportsections=='A' or args.reportsections=='Z': #Computing
            print("Working on section A - Computing")
            section_start_time = datetime.now()
            generate_warehouse_info(snowflake_conn)
            print ("Duration for section A: "+ str(round(  (datetime.now()- section_start_time).total_seconds()/60 ,2) )+" minutes." )

        if args.reportsections=='B' or args.reportsections=='Z': #Storage            
            print("Working on section B - Storage")
            section_start_time = datetime.now()
            table_history_top_tables_by_storage(snowflake_conn)
            table_history_top_database_by_storage(snowflake_conn)
            line_history_top_storage_by_database(snowflake_conn)
            line_history_storage_stages(snowflake_conn)
            print ("Duration for section B: "+ str(round(  (datetime.now()- section_start_time).total_seconds()/60 ,2) )+" minutes." )

        if args.reportsections=='C' or args.reportsections=='Z': #Credits
            print("Working on section C - Credits")
            section_start_time = datetime.now()
            line_history_account_consumption_credits_by_warehouse(snowflake_conn)
            line_history_account_consumption_credits(snowflake_conn)
            bar_month_consumption_credits_by_warehouse(snowflake_conn)
            bar_week_consumption_credits_by_warehouse(snowflake_conn)   
            line_history_daily_credits_used_by_service(snowflake_conn) 
            bar_month_credits_used_by_service(snowflake_conn)
            bar_week_credits_used_by_service(snowflake_conn)
            line_history_credits_replication_by_database(snowflake_conn)
            print ("Duration for section C: "+ str(round(  (datetime.now()- section_start_time).total_seconds()/60 ,2) )+" minutes." )

        if args.reportsections=='D' or args.reportsections=='Z': #Performance
            print("Working on section D - Performance")
            section_start_time = datetime.now()
            table_month_top_query(snowflake_conn)    
            table_week_top_query(snowflake_conn)
            table_history_top_table_by_pruning_efficiency(snowflake_conn)
            table_history_top_table_by_reclustering(snowflake_conn)
            generate_top_query_info(snowflake_conn)
            print ("Duration for section D: "+ str(round(  (datetime.now()- section_start_time).total_seconds()/60 ,2) )+" minutes." )

        if args.reportsections=='E' or args.reportsections=='Z': #Security
            print("Working on section E - Security")
            section_start_time = datetime.now()
            table_history_failed_login(snowflake_conn)
            table_month_new_login(snowflake_conn)
            table_week_new_login(snowflake_conn)
            table_day_new_login(snowflake_conn)
            table_less_frequent_logins(snowflake_conn)
            table_history_users_with_highest_privileges(snowflake_conn)
            table_history_recent_changed_network_policies(snowflake_conn)
            table_history_recent_changed_network_rules(snowflake_conn)
            table_history_recent_changed_password_policies(snowflake_conn)
            table_history_recent_changed_masking_policies(snowflake_conn)
            table_history_recent_changed_row_access_policies(snowflake_conn)
            table_history_users_with_recent_password_changes(snowflake_conn)
            bar_month_sessions_by_authentication_method(snowflake_conn)
            bar_week_sessions_by_authentication_method(snowflake_conn)
            line_history_sessions_by_authentication_method(snowflake_conn)
            line_month_top_logins_by_users(snowflake_conn)
            table_history_ip_changes(snowflake_conn)
            table_history_client_driver_changes(snowflake_conn)
            print ("Duration for section E: "+ str(round(  (datetime.now()- section_start_time).total_seconds()/60 ,2) )+" minutes." )

        if args.reportsections=='F' or args.reportsections=='Z': #Data_Transfer
            print("Working on section F - Data_Transfer")
            section_start_time = datetime.now()
            table_history_top_cloud_data_transfer(snowflake_conn)
            line_history_data_transfer_by_cloud(snowflake_conn)
            line_history_bytes_replication_by_database(snowflake_conn)
            table_history_external_functions(snowflake_conn)
            print ("Duration for section F: "+ str(round(  (datetime.now()- section_start_time).total_seconds()/60 ,2) )+" minutes." )

        if args.reportsections=='G' or args.reportsections=='Z': #Maintenance
            print("Working on section G - Maintenance")
            section_start_time = datetime.now()
            table_history_less_accessed_objects(snowflake_conn)
            table_history_users_without_sessions_last_6_months(snowflake_conn)
            table_history_users_without_sessions_last_3_months(snowflake_conn)
            table_history_need_attention_tasks(snowflake_conn)
            table_history_need_attention_snowpipes(snowflake_conn)
            table_account_non_default_parameters(snowflake_conn)
            table_warehouse_non_default_parameters(snowflake_conn)
            table_database_non_default_parameters(snowflake_conn)
            table_warehouse_without_activity_in_last_3_months(snowflake_conn)
            table_warehouse_without_activity_in_last_month(snowflake_conn)
            line_history_sql_operations(snowflake_conn)
            line_history_sql_operations_by_database(snowflake_conn)
            print ("Duration for section G: "+ str(round(  (datetime.now()- section_start_time).total_seconds()/60 ,2) )+" minutes." )

        if args.reportsections=='H' or args.reportsections=='Z': #DBT
            print("Working on section H - DBT")
            section_start_time = datetime.now()
            table_history_top_dbt_models(snowflake_conn)
            table_month_top_dbt_models(snowflake_conn)
            table_week_top_dbt_models(snowflake_conn)
            print ("Duration for section H: "+ str(round(  (datetime.now()- section_start_time).total_seconds()/60 ,2) )+" minutes." )

    close_snowflake_db_connection(snowflake_conn)
    report_builder()
    print ("\nCompleted. Open the main page: "+report_root_folder+"/prismafy_index.html\n")
    print ("Duration Prismafy report: "+ str(round(  (datetime.now()- report_start_time).total_seconds()/60 ,2) )+" minutes." )

if __name__ == "__main__":
    main()
