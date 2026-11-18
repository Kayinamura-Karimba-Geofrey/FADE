package cases

import (
	"errors"
	"regexp"
)

var (
	reMSISDN  = regexp.MustCompile(`\b07[0-9]{8}\b`)
	reEmail   = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
	re16Digit = regexp.MustCompile(`\b[0-9]{16}\b`)
)

var ErrRedactedContent = errors.New("note contains sensitive data")

// ScanNote returns an error if the note contains sensitive identifiers.
func ScanNote(text string) error {
	if reMSISDN.MatchString(text) {
		return ErrRedactedContent
	}
	if reEmail.MatchString(text) {
		return ErrRedactedContent
	}
	if re16Digit.MatchString(text) {
		return ErrRedactedContent
	}
	return nil
}
