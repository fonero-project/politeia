// Copyright (c) 2017-2019 The Fonero developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package sharedconfig

import (
	"github.com/fonero-project/fnod/fnoutil"
)

const (
	DefaultDataDirname = "data"
)

var (
	DefaultHomeDir = fnoutil.AppDataDir("politeiad", false)
)
