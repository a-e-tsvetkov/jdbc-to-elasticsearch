# jdbc-to-elasticsearch
JDBC driver for elasticsearch

**WARNING** Very early stage. Don't expect too much. 

## Project goal

Create JDBC driver that use elasticsearch as storage.

### Current state
#### Statements supported
* CREATE TABLE
* SELECT
* INSERT
#### Current limitations
* Only int supported as column type
* Rudimentary `join` support
* No subselect (Not even in plans)
* No functions
* SORT BY not supported (second goal)
* HAVING not supported (Not even in plan)
* Very limited metadata support

#### Curent goal
Support inner join

### Major goal

Make it work with Hibernate.

## Usage

See test-client for usage example.
