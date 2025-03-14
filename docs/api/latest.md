# API Docs - v7.0.19

!!! Info "Tested Siddhi Core version: *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/">5.1.24</a>*"
    It could also support other Siddhi Core minor versions.

## Rdbms

### cud *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">(Stream Processor)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This function performs SQL CUD (INSERT, UPDATE, DELETE) queries on data sources. <br>Note: This function to work data sources should be set at the Siddhi Manager level.<br></p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
rdbms:cud(<STRING> datasource.name, <STRING> query)
rdbms:cud(<STRING> datasource.name, <STRING> query, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> parameter)
rdbms:cud(<STRING> datasource.name, <STRING> query, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> parameter, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> ...)
rdbms:cud(<STRING> datasource.name, <STRING> query, <STRING> transaction.correlation.id)
rdbms:cud(<STRING> datasource.name, <STRING> query, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> parameter, <STRING> transaction.correlation.id)
rdbms:cud(<STRING> datasource.name, <STRING> query, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> parameter, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> ..., <STRING> transaction.correlation.id)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">datasource.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the datasource for which the query should be performed. If Siddhi is used as a Java/Python library the datasource should be explicitly set in the siddhi manager in order for the function to work.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">query</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The update, delete, or insert query(formatted according to the relevant database type) that needs to be performed.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">parameter</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If the second parameter is a parametrised SQL query, then siddhi attributes can be passed to set the values of the parameters</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING<br>BOOL<br>INT<br>DOUBLE<br>FLOAT<br>LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">transaction.correlation.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If provided, CUD functions having the same <code>transaction.correlation.id</code> will use the same connection object when interacting with the database. The connection object will not be closed until a <code>commit</code> or <code>rollback</code> query is explicitly performed via a CUD function. This is useful when performing transactions with commit and rollback. CUD functions without a <code>transaction.correlation.id</code> will use their own connection object, which will be closed at the end of the operation. Note that, when using <code>transaction.correlation.id</code>, the developer should make sure that, a <code>commit</code> or <code>rollback</code> operation is performed via a CUD operation, after all the events - that are supposed to be committed/rolled back are added to the batch .</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="system-parameters" class="md-typeset" style="display: block; font-weight: bold;">System Parameters</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Parameters</th>
    </tr>
    <tr>
        <td style="vertical-align: top">perform.CUD.operations</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">If this parameter is set to 'true', the RDBMS CUD function is enabled to perform CUD operations.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
    <tr>
        <td style="vertical-align: top">allow.null.params.with.CUD</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">When set to 'true', this parameter allows the RDBMS CUD function to acceptparameters with NULL values. When set to 'false', NULL parameters will not be allowed with CUD functions.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">numRecords</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The number of records manipulated by the query.</p></td>
        <td style="vertical-align: top">INT</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from TriggerStream#rdbms:cud("SAMPLE_DB", "UPDATE Customers_Table SET customerName='abc' where customerName='xyz'") 
select numRecords 
insert into  RecordStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query updates the events from the input stream named 'TriggerStream' with an additional attribute named 'numRecords', of which the value indicates the number of records manipulated. The updated events are inserted into an output stream named 'RecordStream'.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from TriggerStream#rdbms:cud("SAMPLE_DB", "UPDATE Customers_Table SET customerName=? where customerName=?", changedName, previousName) 
select numRecords 
insert into  RecordStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query updates the events from the input stream named 'TriggerStream' with an additional attribute named 'numRecords', of which the value indicates the number of records manipulated. The updated events are inserted into an output stream named 'RecordStream'. Here the values of attributes changedName and previousName in the event will be set to the query.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
from InsertStream#rdbms:cud("SAMPLE_DB", "INSERT INTO Names(name) VALUES (?);", name, "t1")
select name
insert into ignoreStream;

from CommitStream#rdbms:cud("SAMPLE_DB", "COMMIT",  "t1")
select *
insert into ignoreStream2;

from RollbackStream#rdbms:cud("SAMPLE_DB", "ROLLBACK",  "t1")
select *
insert into ignoreStream3;

```
<p></p>
<p style="word-wrap: break-word;margin: 0;"><code>t1</code> is the <code>transactionCorrelationId</code>. Assume the following series of events arriving at <code>InsertStream</code>: <code>{"name": "A"}</code>, <code>{"name": "B"}</code>. <code>A</code> and <code>B</code> will not be immediately committed to the <code>Names</code> table. After these, if an event arrives at <code>CommitStream</code>, <code>A</code> and <code>B</code> will be committed, since the <code>CommitStream</code> performs a <code>COMMIT</code>. Instead of that, if an event arrives at <code>RollbackStream</code>, <code>A</code> and <code>B</code> will be rolled back, since the <code>RollbackStream</code> performs a <code>ROLLBACK</code>.</p>
<p></p>
### procedure *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">(Stream Processor)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This function execute stored procedure and retrieve data to siddhi  . <br>Note: This function to work data sources should be set at the Siddhi Manager level.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
rdbms:procedure(<STRING> datasource.name, <STRING> attribute.definition.list, <STRING> query)
rdbms:procedure(<STRING> datasource.name, <STRING> attribute.definition.list, <STRING> query, <STRING> output.parameter)
rdbms:procedure(<STRING> datasource.name, <STRING> attribute.definition.list, <STRING> query, <STRING> output.parameter, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> parameter)
rdbms:procedure(<STRING> datasource.name, <STRING> attribute.definition.list, <STRING> query, <STRING> output.parameter, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> parameter, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">datasource.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the datasource for which the query should be performed. If Siddhi is used as a Java/Python library the datasource should be explicitly set in the siddhi manager in order for the function to work.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">attribute.definition.list</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is provided as a comma-separated list in the '&lt;AttributeName AttributeType&gt;' format. The SQL query is expected to return the attributes in the given order. e.g., If one attribute is defined here, the SQL query should return one column result set. If more than one column is returned, then the first column is processed. The Siddhi data types supported are 'STRING', 'INT', 'LONG', 'DOUBLE', 'FLOAT', and 'BOOL'. <br>&nbsp;Mapping of the Siddhi data type to the database data type can be done as follows, <br>*Siddhi Datatype* -&gt; *Datasource Datatype*<br><code>STRING</code> -&gt; <code>CHAR</code>,<code>VARCHAR</code>,<code>LONGVARCHAR</code><br><code>INT</code>&nbsp;&nbsp;&nbsp;&nbsp;-&gt; <code>INTEGER</code><br><code>LONG</code>&nbsp;&nbsp;&nbsp;&nbsp;-&gt; <code>BIGINT</code><br><code>DOUBLE</code>-&gt; <code>DOUBLE</code><br><code>FLOAT</code>&nbsp;&nbsp;&nbsp;&nbsp;-&gt; <code>REAL</code><br><code>BOOL</code>&nbsp;&nbsp;&nbsp;&nbsp;-&gt; <code>BIT</code><br></p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">query</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The select query(formatted according to the relevant database type) that needs to be performed</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">output.parameter</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is provided as a comma-separated list in the '&lt;AttributeType&gt;' format. This AttributeType should be supported by the Database data type</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">parameter</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If the second parameter is a parametrised SQL query, then siddhi attributes can be passed to set the values of the parameters</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING<br>BOOL<br>INT<br>DOUBLE<br>FLOAT<br>LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="system-parameters" class="md-typeset" style="display: block; font-weight: bold;">System Parameters</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Parameters</th>
    </tr>
    <tr>
        <td style="vertical-align: top">database_type</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This parameter should be set in deployment yaml file inorder to initialize the procedure function.</p></td>
        <td style="vertical-align: top">WSO2_CARBON_DB:oracle</td>
        <td style="vertical-align: top">WSO2_CARBON_DB:oracle</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">attributeName</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The return attributes will be the ones defined in the parameter<code>attribute.definition.list</code>.</p></td>
        <td style="vertical-align: top">STRING<br>INT<br>LONG<br>DOUBLE<br>FLOAT<br>BOOL</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from IntrimStream#rdbms:procedure('ORACLE_DB', 'Name String, Age int,Date_Time String', 'begin RETURNCON(?,?); end;','cursor', NoOfYears)
select Name, Age, Date_Time
insert into tempStream1;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will execute the stored procedure RETURNCON() which is defined in the database and return the attributes Name, Age and Date_Time as the output. Above RETURNCON() Stored Procedure accepts two parameters, one is a input parameter and the other one is an output parameter, in this case it's a 'cursor'. input parameter value will provided from the IntrimStream and the parameter is NoOfYears</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from IntrimStream#rdbms:procedure('ORACLE_DB', 'Name String, Age int,Date_Time String', 'begin RETURNCON(9,?); end;','cursor')
select Name, Age, Date_Time
insert into tempStream1;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will execute the stored procedure RETURNCON() which is defined in the database and return the attributes Name, Age and Date_Time as the output. Above RETURNCON() Stored Procedure accepts two parameters, one is a input parameter and the other one is an output parameter, in this case it's a 'cursor'. input parameter is provided in the query itself in this example</p>
<p></p>
### query *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">(Stream Processor)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This function performs SQL retrieval queries on data sources. <br>Note: This function to work data sources should be set at the Siddhi Manager level.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
rdbms:query(<STRING> datasource.name, <STRING> attribute.definition.list, <STRING> query)
rdbms:query(<STRING> datasource.name, <STRING> attribute.definition.list, <STRING> query, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> parameter)
rdbms:query(<STRING> datasource.name, <STRING> attribute.definition.list, <STRING> query, <BOOL> ack.empty.result.set)
rdbms:query(<STRING> datasource.name, <STRING> attribute.definition.list, <STRING> query, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> parameter, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> ...)
rdbms:query(<STRING> datasource.name, <STRING> attribute.definition.list, <STRING> query, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> parameter, <STRING|BOOL|INT|DOUBLE|FLOAT|LONG> ..., <BOOL> ack.empty.result.set)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">datasource.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the datasource for which the query should be performed. If Siddhi is used as a Java/Python library the datasource should be explicitly set in the siddhi manager in order for the function to work.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">attribute.definition.list</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is provided as a comma-separated list in the '&lt;AttributeName AttributeType&gt;' format. The SQL query is expected to return the attributes in the given order. e.g., If one attribute is defined here, the SQL query should return one column result set. If more than one column is returned, then the first column is processed. The Siddhi data types supported are 'STRING', 'INT', 'LONG', 'DOUBLE', 'FLOAT', and 'BOOL'. <br>&nbsp;Mapping of the Siddhi data type to the database data type can be done as follows, <br>*Siddhi Datatype* -&gt; *Datasource Datatype*<br><code>STRING</code> -&gt; <code>CHAR</code>,<code>VARCHAR</code>,<code>LONGVARCHAR</code><br><code>INT</code>&nbsp;&nbsp;&nbsp;&nbsp;-&gt; <code>INTEGER</code><br><code>LONG</code>&nbsp;&nbsp;&nbsp;&nbsp;-&gt; <code>BIGINT</code><br><code>DOUBLE</code>-&gt; <code>DOUBLE</code><br><code>FLOAT</code>&nbsp;&nbsp;&nbsp;&nbsp;-&gt; <code>REAL</code><br><code>BOOL</code>&nbsp;&nbsp;&nbsp;&nbsp;-&gt; <code>BIT</code><br></p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">query</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The select query(formatted according to the relevant database type) that needs to be performed</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">parameter</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If the second parameter is a parametrised SQL query, then siddhi attributes can be passed to set the values of the parameters</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING<br>BOOL<br>INT<br>DOUBLE<br>FLOAT<br>LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">ack.empty.result.set</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">When the parameter is set to <code>true</code>, the return attributes will contain <code>null</code> values if the result set is empty. <br>If the parameter is set to <code>false</code>, the function wont return any attributes.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">attributeName</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The return attributes will be the ones defined in the parameter<code>attribute.definition.list</code>.</p></td>
        <td style="vertical-align: top">STRING<br>INT<br>LONG<br>DOUBLE<br>FLOAT<br>BOOL</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from TriggerStream#rdbms:query('SAMPLE_DB', 'creditcardno string, country string, transaction string, amount int', 'select * from Transactions_Table') 
select creditcardno, country, transaction, amount 
insert into recordStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Events inserted into recordStream includes all records matched for the query i.e an event will be generated for each record retrieved from the datasource. The event will include as additional attributes, the attributes defined in the <code>attribute.definition.list</code>(creditcardno, country, transaction, amount).</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from TriggerStream#rdbms:query('SAMPLE_DB', 'creditcardno string, country string,transaction string, amount int', 'select * from where country=?', countrySearchWord) select creditcardno, country, transaction, amount 
insert into recordStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Events inserted into recordStream includes all records matched for the query i.e an event will be generated for each record retrieved from the datasource. The event will include as additional attributes, the attributes defined in the <code>attribute.definition.list</code>(creditcardno, country, transaction, amount). countrySearchWord value from the event will be set in the query when querying the datasource.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
from TriggerStream#rdbms:query('SAMPLE_DB', 'creditcardno string, country string,transaction string, amount int', 'select * from where country=?', countrySearchWord, true) select creditcardno, country, transaction, amount 
insert into recordStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">If there are no events in the table which satisfies the given query with <code>creditcardno</code> parameter, the event which gets selected by creditcardno, country, transaction and amount will contain NULL values.</p>
<p></p>
## Store

### rdbms *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#store">(Store)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This extension assigns data sources and connection instructions to event tables. It also implements read-write operations on connected data sources. A new improvement is added when running with SI / SI Tooling 1.1.0 or higher product pack, where an external configuration file can be provided to read supported RDBMS databases. Prerequisites - Configuration file needed to be added to [Product_Home]/conf/siddhi/rdbms path with the configuration file name as rdbms-table-config.xml , &lt;database name=”[Database_Name]”&gt; for each database name should be the equivalent database product name returned from java sql Connection.getMetaData().getDatabaseProductName() as shown in API documentation  https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getDatabaseProductName()).Sample Configuration for one of the databases can be as follows,&lt;?xml version="1.0" encoding="UTF-8" standalone="yes"?&gt;<br>&lt;rdbms-table-configuration&gt;<br>&lt;database name="Teradata"&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;tableCreateQuery&gt;CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})&lt;/tableCreateQuery&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;tableCheckQuery&gt;SELECT 1 FROM {{TABLE_NAME}} SAMPLE 1&lt;/tableCheckQuery&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;indexCreateQuery&gt;CREATE INDEX {{TABLE_NAME}}_INDEX_{{INDEX_NUM}} ({{INDEX_COLUMNS}}) ON {{TABLE_NAME}}<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/indexCreateQuery&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;recordExistsQuery&gt;SELECT 1 FROM {{TABLE_NAME}} {{CONDITION}} SAMPLE 1&lt;/recordExistsQuery&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;recordSelectQuery&gt;SELECT * FROM {{TABLE_NAME}} {{CONDITION}}&lt;/recordSelectQuery&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;recordInsertQuery&gt;INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})&lt;/recordInsertQuery&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;recordUpdateQuery&gt;UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}&lt;/recordUpdateQuery&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;recordDeleteQuery&gt;DELETE FROM {{TABLE_NAME}} {{CONDITION}}&lt;/recordDeleteQuery&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;recordContainsCondition&gt;({{COLUMNS}} LIKE {{VALUES}})&lt;/recordContainsCondition&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;selectQueryTemplate&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;selectClause&gt;SELECT {{SELECTORS}} FROM {{TABLE_NAME}}&lt;/selectClause&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;selectQueryWithSubSelect&gt;SELECT {{SELECTORS}} FROM {{TABLE_NAME}}, ( {{INNER_QUERY}} ) AS t2<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/selectQueryWithSubSelect&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;whereClause&gt;WHERE {{CONDITION}}&lt;/whereClause&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;groupByClause&gt;GROUP BY {{COLUMNS}}&lt;/groupByClause&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;havingClause&gt;HAVING {{CONDITION}}&lt;/havingClause&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;orderByClause&gt;ORDER BY {{COLUMNS}}&lt;/orderByClause&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;limitClause&gt;SAMPLE {{Q}}&lt;/limitClause&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/selectQueryTemplate&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;stringSize&gt;254&lt;/stringSize&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;batchEnable&gt;true&lt;/batchEnable&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;batchSize&gt;1000&lt;/batchSize&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeMapping&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;binaryType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeName&gt;BLOB&lt;/typeName&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeValue&gt;2004&lt;/typeValue&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/binaryType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;booleanType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeName&gt;SMALLINT&lt;/typeName&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeValue&gt;5&lt;/typeValue&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/booleanType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;doubleType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeName&gt;FLOAT&lt;/typeName&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeValue&gt;8&lt;/typeValue&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/doubleType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;floatType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeName&gt;FLOAT&lt;/typeName&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeValue&gt;6&lt;/typeValue&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/floatType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;integerType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeName&gt;INTEGER&lt;/typeName&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeValue&gt;4&lt;/typeValue&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/integerType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;longType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeName&gt;BIGINT&lt;/typeName&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeValue&gt;-5&lt;/typeValue&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/longType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;stringType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeName&gt;VARCHAR&lt;/typeName&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;typeValue&gt;12&lt;/typeValue&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/stringType&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/typeMapping&gt;<br>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/database&gt;<br>&lt;/rdbms-table-configuration&gt;</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@Store(type="rdbms", jdbc.url="<STRING>", username="<STRING>", password="<STRING>", jdbc.driver.name="<STRING>", pool.properties="<STRING>", jndi.resource="<STRING>", datasource="<STRING>", table.name="<STRING>", field.length="<STRING>", table.check.query="<STRING>", use.collation="<BOOL>", allow.null.values="<BOOL>")
@PrimaryKey("PRIMARY_KEY")
@Index("INDEX")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">jdbc.url</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The JDBC URL via which the RDBMS data store is accessed.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The username to be used to access the RDBMS data store.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The password to be used to access the RDBMS data store.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">jdbc.driver.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The driver class name for connecting the RDBMS data store.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">pool.properties</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Any pool parameters for the database connection must be specified as key-value pairs.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">jndi.resource</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the JNDI resource through which the connection is attempted. If this is found, the pool properties described above are not taken into account and the connection is attempted via JNDI lookup instead.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">datasource</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the Carbon datasource that should be used for creating the connection with the database. If this is found, neither the pool properties nor the JNDI resource name described above are taken into account and the connection is attempted via Carbon datasources instead. Only works in Siddhi Distribution </p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name with which the event table should be persisted in the store. If no name is specified via this parameter, the event table is persisted with the same name as the Siddhi table.</p></td>
        <td style="vertical-align: top">The table name defined in the Siddhi App query.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">field.length</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The number of characters that the values for fields of the 'STRING' type in the table definition must contain. Each required field must be provided as a comma-separated list of key-value pairs in the '&lt;field.name&gt;:&lt;length&gt;' format. If this is not specified, the default number of characters specific to the database type is considered.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.check.query</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This query will be used to check whether the table is exist in the given database. But the provided query should return an SQLException if the table does not exist in the database. Furthermore if the provided table is a database view, and it is not exists in the database a table from given name will be created in the database</p></td>
        <td style="vertical-align: top">The tableCheckQuery which define in store rdbms configs</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">use.collation</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This property allows users to use collation for string attributes. By default it's false and binary collation is not used. Currently 'latin1_bin' and 'SQL_Latin1_General_CP1_CS_AS' are used as collations for MySQL and Microsoft SQL database types respectively.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">allow.null.values</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This property allows users to insert null values to the numeric columns. </p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="system-parameters" class="md-typeset" style="display: block; font-weight: bold;">System Parameters</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Parameters</th>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.maxVersion</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The latest version supported for {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.minVersion</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The earliest version supported for {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.tableCheckQuery</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The template query for the 'check table' operation in {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top"><b>H2</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})<br><b>MySQL</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})<br><b>Oracle</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})<br><b>Microsoft SQL Server</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})<br><b>PostgreSQL</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})<br><b>DB2.*</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.tableCreateQuery</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The template query for the 'create table' operation in {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top"><b>H2</b>: SELECT 1 FROM {{TABLE_NAME}} LIMIT 1<br><b>MySQL</b>: SELECT 1 FROM {{TABLE_NAME}} LIMIT 1<br><b>Oracle</b>: SELECT 1 FROM {{TABLE_NAME}} WHERE rownum=1<br><b>Microsoft SQL Server</b>: SELECT TOP 1 1 from {{TABLE_NAME}}<br><b>PostgreSQL</b>: SELECT 1 FROM {{TABLE_NAME}} LIMIT 1<br><b>DB2.*</b>: SELECT 1 FROM {{TABLE_NAME}} FETCH FIRST 1 ROWS ONLY</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.indexCreateQuery</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The template query for the 'create index' operation in {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top"><b>H2</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} ({{INDEX_COLUMNS}})<br><b>MySQL</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} ({{INDEX_COLUMNS}})<br><b>Oracle</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} ({{INDEX_COLUMNS}})<br><b>Microsoft SQL Server</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} ({{INDEX_COLUMNS}}) {{TABLE_NAME}} ({{INDEX_COLUMNS}})<br><b>PostgreSQL</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} ({{INDEX_COLUMNS}})<br><b>DB2.*</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} ({{INDEX_COLUMNS}})</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.recordInsertQuery</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The template query for the 'insert record' operation in {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top"><b>H2</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})<br><b>MySQL</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})<br><b>Oracle</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})<br><b>Microsoft SQL Server</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})<br><b>PostgreSQL</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})<br><b>DB2.*</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.recordUpdateQuery</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The template query for the 'update record' operation in {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top"><b>H2</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}<br><b>MySQL</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}<br><b>Oracle</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}<br><b>Microsoft SQL Server</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}<br><b>PostgreSQL</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}<br><b>DB2.*</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.recordSelectQuery</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The template query for the 'select record' operation in {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top"><b>H2</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}<br><b>MySQL</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}<br><b>Oracle</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}<br><b>Microsoft SQL Server</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}<br><b>PostgreSQL</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}<br><b>DB2.*</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.recordExistsQuery</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The template query for the 'check record existence' operation in {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top"><b>H2</b>: SELECT TOP 1 1 FROM {{TABLE_NAME}} {{CONDITION}}<br><b>MySQL</b>: SELECT 1 FROM {{TABLE_NAME}} {{CONDITION}}<br><b>Oracle</b>: SELECT COUNT(1) INTO existence FROM {{TABLE_NAME}} {{CONDITION}}<br><b>Microsoft SQL Server</b>: SELECT TOP 1 FROM {{TABLE_NAME}} {{CONDITION}}<br><b>PostgreSQL</b>: SELECT 1 FROM {{TABLE_NAME}} {{CONDITION}} LIMIT 1<br><b>DB2.*</b>: SELECT 1 FROM {{TABLE_NAME}} {{CONDITION}} FETCH FIRST 1 ROWS ONLY</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.recordDeleteQuery</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The query for the 'delete record' operation in {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top"><b>H2</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}<br><b>MySQL</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}<br><b>Oracle</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}<br><b>Microsoft SQL Server</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}<br><b>PostgreSQL</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}<br><b>DB2.*</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.stringSize</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This defines the length for the string fields in {{RDBMS-Name}}.</p></td>
        <td style="vertical-align: top"><b>H2</b>: 254<br><b>MySQL</b>: 254<br><b>Oracle</b>: 254<br><b>Microsoft SQL Server</b>: 254<br><b>PostgreSQL</b>: 254<br><b>DB2.*</b>: 254</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.fieldSizeLimit</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This defines the field size limit for select/switch to big string type from the default string type if the 'bigStringType' is available in field type list.</p></td>
        <td style="vertical-align: top"><b>H2</b>: N/A<br><b>MySQL</b>: N/A<br><b>Oracle</b>: 2000<br><b>Microsoft SQL Server</b>: N/A<br><b>PostgreSQL</b>: N/A<br><b>DB2.*</b>: N/A</td>
        <td style="vertical-align: top">0 =< n =< INT_MAX</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.batchSize</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This defines the batch size when operations are performed for batches of events.</p></td>
        <td style="vertical-align: top"><b>H2</b>: 1000<br><b>MySQL</b>: 1000<br><b>Oracle</b>: 1000<br><b>Microsoft SQL Server</b>: 1000<br><b>PostgreSQL</b>: 1000<br><b>DB2.*</b>: 1000</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.batchEnable</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This specifies whether 'Update' and 'Insert' operations can be performed for batches of events or not.</p></td>
        <td style="vertical-align: top"><b>H2</b>: true<br><b>MySQL</b>: true<br><b>Oracle (versions 12.0 and less)</b>: false<br><b>Oracle (versions 12.1 and above)</b>: true<br><b>Microsoft SQL Server</b>: true<br><b>PostgreSQL</b>: true<br><b>DB2.*</b>: true</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.transactionSupported</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is used to specify whether the JDBC connection that is used supports JDBC transactions or not.</p></td>
        <td style="vertical-align: top"><b>H2</b>: true<br><b>MySQL</b>: true<br><b>Oracle</b>: true<br><b>Microsoft SQL Server</b>: true<br><b>PostgreSQL</b>: true<br><b>DB2.*</b>: true</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.typeMapping.binaryType</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is used to specify the binary data type. An attribute defines as 'object' type in Siddhi stream will be stored into RDBMS with this type.</p></td>
        <td style="vertical-align: top"><b>H2</b>: BLOB<br><b>MySQL</b>: BLOB<br><b>Oracle</b>: BLOB<br><b>Microsoft SQL Server</b>: VARBINARY(max)<br><b>PostgreSQL</b>: BYTEA<br><b>DB2.*</b>: BLOB(64000)</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.typeMapping.booleanType</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is used to specify the boolean data type. An attribute defines as 'bool' type in Siddhi stream will be stored into RDBMS with this type.</p></td>
        <td style="vertical-align: top"><b>H2</b>: TINYINT(1)<br><b>MySQL</b>: TINYINT(1)<br><b>Oracle</b>: NUMBER(1)<br><b>Microsoft SQL Server</b>: BIT<br><b>PostgreSQL</b>: BOOLEAN<br><b>DB2.*</b>: SMALLINT</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.typeMapping.doubleType</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is used to specify the double data type. An attribute defines as 'double' type in Siddhi stream will be stored into RDBMS with this type.</p></td>
        <td style="vertical-align: top"><b>H2</b>: DOUBLE<br><b>MySQL</b>: DOUBLE<br><b>Oracle</b>: NUMBER(19,4)<br><b>Microsoft SQL Server</b>: FLOAT(32)<br><b>PostgreSQL</b>: DOUBLE PRECISION<br><b>DB2.*</b>: DOUBLE</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.typeMapping.floatType</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is used to specify the float data type. An attribute defines as 'float' type in Siddhi stream will be stored into RDBMS with this type.</p></td>
        <td style="vertical-align: top"><b>H2</b>: FLOAT<br><b>MySQL</b>: FLOAT<br><b>Oracle</b>: NUMBER(19,4)<br><b>Microsoft SQL Server</b>: REAL<br><b>PostgreSQL</b>: REAL<br><b>DB2.*</b>: REAL</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.typeMapping.integerType</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is used to specify the integer data type. An attribute defines as 'int' type in Siddhi stream will be stored into RDBMS with this type.</p></td>
        <td style="vertical-align: top"><b>H2</b>: INTEGER<br><b>MySQL</b>: INTEGER<br><b>Oracle</b>: NUMBER(10)<br><b>Microsoft SQL Server</b>: INTEGER<br><b>PostgreSQL</b>: INTEGER<br><b>DB2.*</b>: INTEGER</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.typeMapping.longType</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is used to specify the long data type. An attribute defines as 'long' type in Siddhi stream will be stored into RDBMS with this type.</p></td>
        <td style="vertical-align: top"><b>H2</b>: BIGINT<br><b>MySQL</b>: BIGINT<br><b>Oracle</b>: NUMBER(19)<br><b>Microsoft SQL Server</b>: BIGINT<br><b>PostgreSQL</b>: BIGINT<br><b>DB2.*</b>: BIGINT</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.typeMapping.stringType</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is used to specify the string data type. An attribute defines as 'string' type in Siddhi stream will be stored into RDBMS with this type.</p></td>
        <td style="vertical-align: top"><b>H2</b>: VARCHAR(stringSize)<br><b>MySQL</b>: VARCHAR(stringSize)<br><b>Oracle</b>: VARCHAR(stringSize)<br><b>Microsoft SQL Server</b>: VARCHAR(stringSize)<br><b>PostgreSQL</b>: VARCHAR(stringSize)<br><b>DB2.*</b>: VARCHAR(stringSize)</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
    <tr>
        <td style="vertical-align: top">{{RDBMS-Name}}.typeMapping.bigStringType</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is used to specify the big string data type. An attribute defines as 'string' type in Siddhi stream and field.length define in the annotation is greater than the fieldSizeLimit, will be stored into RDBMS with this type.</p></td>
        <td style="vertical-align: top"><b>H2</b>: N/A<br><b>MySQL</b>: N/A<b>Oracle</b>: CLOB<b>Microsoft SQL Server</b>: N/A<br><b>PostgreSQL</b>: N/A<br><b>DB2.*</b>: N/A</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@Store(type="rdbms", jdbc.url="jdbc:mysql://localhost:3306/stocks", username="root", password="root", jdbc.driver.name="com.mysql.jdbc.Driver",field.length="symbol:100")
@PrimaryKey("id", "symbol")
@Index("volume")
define table StockTable (id string, symbol string, price float, volume long);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">The above example creates an event table named 'StockTable' in the database if it does not already exist (with four attributes named <code>id</code>, <code>symbol</code>, <code>price</code>, and <code>volume</code> of the types 'string', 'string', 'float', and 'long' respectively). The connection is made as specified by the parameters configured for the '@Store' annotation.<br><br>&nbsp;The @PrimaryKey() and @Index() annotations can be used to define primary keys or indexes for the table and they follow Siddhi query syntax. RDBMS store supports having more than one <code>attributes</code> in the @PrimaryKey or @Index annotations.<br>&nbsp;In this example a composite Primary key of both attributes <code>id</code> and <code>symbol</code> will be created.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@Store(type="rdbms", jdbc.url="jdbc:mysql://localhost:3306/das", username="root", password="root" , jdbc.driver.name="org.h2.Driver",field.length="symbol:100")
@PrimaryKey("symbol")
@Index("symbol")
define table StockTable (symbol string, price float, volume long);
define stream InputStream (symbol string, volume long);
from InputStream as a join StockTable as b on str:contains(b.symbol, a.symbol)
select a.symbol as symbol, b.volume as volume
insert into FooStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">The above example creates an event table named 'StockTable' in the database if it does not already exist (with three attributes named 'symbol', 'price', and 'volume' of the types 'string', 'float' and 'long' respectively). Then the table is joined with a stream named 'InputStream' based on a condition. The following operations are included in the condition:<br>[ AND, OR, Comparisons( &lt;  &lt;=  &gt;  &gt;=  == !=), IS NULL, NOT, str:contains(Table&lt;Column&gt;, Stream&lt;Attribute&gt; or Search.String)]</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@Store(type="rdbms", jdbc.url="jdbc:mysql://localhost:3306/das", table.name="StockTable", username="root", password="root" , jdbc.driver.name="org.h2.Driver", field.length="symbol:100", table.check.query="SELECT 1 FROM StockTable LIMIT 1")
@PrimaryKey("symbol")
@Index("symbol")
define table StockTable (symbol string, price float, volume long);
define stream InputStream (symbol string, volume long);
from InputStream as a join StockTable as b on str:contains(b.symbol, a.symbol)
select a.symbol as symbol, b.volume as volume
insert into FooStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">The above example creates an event table named 'StockTable' in the database if it does not already exist (with three attributes named 'symbol', 'price', and 'volume' of the types 'string', 'float' and 'long' respectively). Then the table is joined with a stream named 'InputStream' based on a condition. The following operations are included in the condition:<br>[ AND, OR, Comparisons( &lt;  &lt;=  &gt;  &gt;=  == !=), IS NULL, NOT, str:contains(Table&lt;Column&gt;, Stream&lt;Attribute&gt; or Search.String)]</p>
<p></p>
