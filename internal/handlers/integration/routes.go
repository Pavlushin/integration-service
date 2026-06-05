package integration

import "onec-integration/internal/gateway"

func RegisterRoutes(router *gateway.Router) {
	RegisterUsersHighReq(router)
}
