About
-----

The sphinx(full text search server) client package for the Go programming language.

## Installation

`go get github.com/yunge/sphinx`


## Testing

Import "documents.sql" to "test" database in mysql;

Change the mysql password in sphinx.conf;

Copy the test.xml to default dir in sphinx.conf:
`cp test.xml /usr/local/sphinx/var/data`

Index the test data:
`indexer -c sphinx_lib_path/sphinx.conf --all --rotate`

Start sphinx searchd with "sphinx.conf":
`searchd -c sphinx_lib_path/sphinx.conf`

Then "cd" to sphinx_lib_path:

`go test`

## Examples
```Go
import (
  "github.com/yunge/sphinx"
)

// Get sphinx client
opts := &Options{
	Host:    host,
	Port:    9312,
	Timeout: 5000,
}

sc := sphinx.NewClient(opts)

// Or use this style:
// Note: SetServer("", 0) means use default value.
sc := sphinx.NewClient().SetServer(host, 0).SetConnectTimeout(5000)
if err := sc.Error(); err != nil {
	// handle err
}

res, err := sc.Query(words, index, "Some comment...")
if err != nil {
	// handle err
}

for _, match := range res.Matches {
	// handle match.DocId
}

```
More examples can be found in test files.

## LICENSE

BSD License
[http://opensource.org/licenses/bsd-license](http://opensource.org/licenses/bsd-license)
