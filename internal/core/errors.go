package core

import (
	"errors"
	"fmt"
)

type Code string

const (
	Invalid         Code = "BAD_USER_INPUT"
	NotFound        Code = "NOT_FOUND"
	Forbidden       Code = "FORBIDDEN"
	Unauthenticated Code = "UNAUTHENTICATED"
	Unavailable     Code = "UNAVAILABLE"
)

type Error struct {
	Code    Code
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}
func (e *Error) Unwrap() error             { return e.Cause }
func Fail(code Code, message string) error { return &Error{Code: code, Message: message} }
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return Unavailable
}
func StorageError(err error) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return err
	}
	return &Error{Code: Unavailable, Message: "storage temporarily unavailable", Cause: err}
}
