package sod

type Object interface {
	UUID() string
	Initialize(string)
}

type Item struct {
	uuid string
}

func (o *Item) Initialize(uuid string) {
	o.uuid = uuid
}

func (o *Item) UUID() string {
	return o.uuid
}
