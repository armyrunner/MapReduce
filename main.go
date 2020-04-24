package main

import(
<<<<<<< HEAD

	"runtime"

)


func main() {

	runtime.GOMAXPROCS(1)

=======
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(1)
>>>>>>> a7c005698c38f67807f69d97ba5582a74ee488c2
	TestDatabase()
}

