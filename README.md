# btx

btx provides golang struct field tag support for Google [Bigtable](https://pkg.go.dev/google.golang.org/cloud/bigtable).


The field is `bigtable` and the the value is a colon-delimited value, ie. `<FAMILY>:<COLUMN_NAME>`.

```
type Person struct {
    FirstName string `bigtable:"person:first" json:"first"`
    LastName string `bigtable:"person:last" json:"last"`
}

type Employee struct {
    Person Person
    ID              int32 `bigtable:"emp:id"`
}


func demoMarhsal() {
    emp := Employee{
        Person: Person{FirstName: "alice"},
        ID: 1234,
    }
    bmu, err := NewRowMutation(&emp, time.Now())
    println("size of row", bmu.Size)
    // use as  table.Apply(ctx, key, bmu.Mut)
}

func demoUnmarshal() {
    var emp Employee
    // row is a bigtable.Row
    err := UnmarshalRow(row, &emp)
    println(emp.ID, emp.Person.Fname)
}
```

## Row key as value

Example:

```
type Transaction struct {
    ID string `bigtable:,rowkey"`
    User string `bigtable:"cf1:user"`
}
```

This is used in unmarshal. It does nothing for marshal.

## Key/Value support

> EXPERIMENTAL / WIP

In some cases we want to support struts that identify the family but
allow the column name to be dynamic.

```
type MappedThing struct {
    Labels map[string]string  `bigtable:"labels:$$"`
}
```

Here the column family of `labels` is a YOLO zone. We expect that this family is not
used in any other way; it should be wholly owned and used by `Labels`.

### Support Matrix

```
raw bytes
String
Bool
Int
Int8
Int16
Int32
Uint
Uint8
Uint16
Uint32
Float32
Float64
```
