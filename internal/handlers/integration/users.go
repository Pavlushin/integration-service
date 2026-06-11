package integration

import (
	"net/http"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/gateway"
	"onec-integration/internal/worksheets/usersreports"
)

func RegisterWorksheetsUsersReportsHighReq(router *gateway.Router) {
	registerWorksheetsUsersReportsRoute(router, "/integration/api/v1/worksheets/users-reports/export")
}

func RegisterLegacyUsersHighReq(router *gateway.Router) {
	registerWorksheetsUsersReportsRoute(router, "/integration/api/v1/users")
}

func registerWorksheetsUsersReportsRoute(router *gateway.Router, path string) {
	gateway.RegisterHigh(router, gateway.HighOptions[usersreports.Request]{
		Method:    http.MethodPost,
		Path:      path,
		Source:    "integration_api",
		Type:      "worksheets_users_reports_export",
		Direction: enginejob.DirectionInbound,
		BuildDedupe: func(req usersreports.Request) (string, error) {
			return usersreports.BuildDedupeKey(req), nil
		},
	})
}
