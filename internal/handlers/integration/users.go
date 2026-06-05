package integration

import (
	"fmt"
	"net/http"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/gateway"
	"onec-integration/internal/worksheetsexport"
)

func RegisterUsersHighReq(router *gateway.Router) {
	gateway.RegisterHigh(router, gateway.HighOptions[worksheetsexport.Request]{
		Method:    http.MethodPost,
		Path:      "/integration/api/v1/users",
		Source:    "integration_api",
		Type:      "worksheets_export_test",
		Direction: enginejob.DirectionInbound,
		BuildDedupe: func(req worksheetsexport.Request) (string, error) {
			return fmt.Sprintf("worksheets_export_test:%s:%s", req.DateFrom, req.DateTo), nil
		},
	})
}
