package dict

type Dict interface {
	Get(key string) (val any, exists bool)
	Put(key string, val any) (result int)
	Delete(key string) int
	PutIfNX(key string, val any) (result int)
	Len() int
}
