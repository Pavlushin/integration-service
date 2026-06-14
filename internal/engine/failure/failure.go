package failure

import "errors"

type nonRetryableMarker interface {
	NonRetryable() bool
}

type nonRetryableError struct {
	err error
}

func (e nonRetryableError) Error() string {
	return e.err.Error()
}

func (e nonRetryableError) Unwrap() error {
	return e.err
}

func (e nonRetryableError) NonRetryable() bool {
	return true
}

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	if IsNonRetryable(err) {
		return err
	}
	return nonRetryableError{err: err}
}

func IsNonRetryable(err error) bool {
	var marker nonRetryableMarker
	return errors.As(err, &marker) && marker.NonRetryable()
}
