Siddhi Store RDBMS
===================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-rdbms/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-rdbms/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-store-rdbms.svg)](https://github.com/siddhi-io/siddhi-store-rdbms/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-store-rdbms.svg)](https://github.com/siddhi-io/siddhi-store-rdbms/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-store-rdbms.svg)](https://github.com/siddhi-io/siddhi-store-rdbms/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-store-rdbms.svg)](https://github.com/siddhi-io/siddhi-store-rdbms/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-store-rdbms extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that persist and retrieve events to/from RDBMS databases such as MySQL, MS SQL, PostgreSQL, H2 and Oracle.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 6.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.store.rdbms/siddhi-store-rdbms/">here</a>.
* Versions 5.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.store.rdbms/siddhi-store-rdbms">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-rdbms/api/6.0.2">6.0.2</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-rdbms/api/6.0.2/#cud-stream-processor">cud</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#stream-processor">(Stream Processor)</a>*<br><div style="padding-left: 1em;"><p>This function performs SQL CUD (INSERT, UPDATE, DELETE) queries on WSO2 datasources. <br>Note: This function is only available when running Siddhi with WSO2 SP.<br></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-rdbms/api/6.0.2/#query-stream-processor">query</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#stream-processor">(Stream Processor)</a>*<br><div style="padding-left: 1em;"><p>This function performs SQL retrieval queries on WSO2 datasources. <br>Note: This function is only available when running Siddhi with WSO2 SP.</p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-rdbms/api/6.0.2/#rdbms-store">rdbms</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#store">(Store)</a>*<br><div style="padding-left: 1em;"><p>This extension assigns data sources and connection instructions to event tables. It also implements read-write operations on connected datasources.</p></div>

## Dependencies 

JDBC connector jar should be added to the runtime. Download the JDBC connector jar based on the RDBMS type that is connected through the Siddhi store. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

