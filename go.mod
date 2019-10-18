module github.com/itchio/wharf

go 1.13

require (
	github.com/golang/protobuf v1.3.1
	github.com/hashicorp/golang-lru v0.5.1
	github.com/itchio/arkive v0.0.0-20190910190734-aa506bf6be35
	github.com/itchio/go-brotli v0.0.0-20190702114328-3f28d645a45c
	github.com/itchio/headway v0.0.0-20191015112415-46f64dd4d524
	github.com/itchio/httpkit v0.0.0-20191016123402-68159f3a0f00
	github.com/itchio/lake v0.0.0-20191018150143-c34c3b5e550f
	github.com/itchio/randsource v0.0.0-20190703104731-3f6d22f91927
	github.com/itchio/savior v0.0.0-20190702184736-b8b849654d01
	github.com/itchio/screw v0.0.0-20191018142458-f361f6d1fb67
	github.com/jgallagher/gosaca v0.0.0-20130226042358-754749770f08
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.4.0
)

replace github.com/itchio/lake => ../lake
