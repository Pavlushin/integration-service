package worksheetsexport

type Request struct {
	DateFrom string `json:"date_from" validate:"required,datetime=2006-01-02"`
	DateTo   string `json:"date_to" validate:"required,datetime=2006-01-02"`
}

type DeliveryPayload struct {
	PrepareJobID string `json:"prepare_job_id"`
	TempPath     string `json:"temp_path"`
}
