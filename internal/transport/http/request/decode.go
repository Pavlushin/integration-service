package request

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-playground/validator/v10"
)

var requestValidator = validator.New()

func DecodeAndValidate(r *http.Request, dest any) error {
	if err := json.NewDecoder(r.Body).Decode(dest); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}

	if err := requestValidator.Struct(dest); err != nil {
		return fmt.Errorf("validate request: %w", err)
	}

	return nil
}
