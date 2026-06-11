package usersreports

import "fmt"

type Request struct {
	DateFrom       string  `json:"date_from" validate:"required,datetime=2006-01-02"`
	DateTo         string  `json:"date_to" validate:"required,datetime=2006-01-02"`
	LocationCode1C *string `json:"location_code_1c,omitempty"`
	EmployeeCode1C *string `json:"employee_code_1c,omitempty"`
}

type DeliveryPayload struct {
	PrepareJobID string `json:"prepare_job_id"`
	TempPath     string `json:"temp_path"`
}

func BuildDedupeKey(req Request) string {
	return fmt.Sprintf(
		"worksheets_users_reports:%s:%s:%s:%s",
		req.DateFrom,
		req.DateTo,
		nullableString(req.LocationCode1C),
		nullableString(req.EmployeeCode1C),
	)
}

func nullableString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
