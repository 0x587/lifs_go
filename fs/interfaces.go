package fs

type IF interface {
	Mount(dir string) (unmountFunc func(), err error)
}
