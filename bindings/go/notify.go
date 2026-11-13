package fusee

// This file exports the notify callback; with //export, its preamble may
// hold declarations only.

/*
#include <stddef.h>
*/
import "C"

import (
	"sync"
	"unsafe"
)

// Open engines by their notify key (a malloc'd address passed as the
// callback's argument). A callback that races Close finds nothing.
var engines sync.Map

//export fuseeGoNotify
func fuseeGoNotify(user unsafe.Pointer) {
	if e, ok := engines.Load(uintptr(user)); ok {
		e.(*Engine).notify()
	}
}
