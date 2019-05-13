package sqlchemy

import (
	"errors"
)

var ErrNoDataToUpdate error
var ErrDuplicateEntry error
var ErrEmptyQuery error

func init() {
	ErrNoDataToUpdate = errors.New("No data to update")
	ErrDuplicateEntry = errors.New("duplicate entry")
	ErrEmptyQuery = errors.New("empty query")
}
