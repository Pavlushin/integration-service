package usersreports

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestExportLoadsUserReportsBySelectedMainReportIDs(t *testing.T) {
	storage := &exportStorageFake{
		links: []LinkRow{
			{LinkID: 10, LinkName: "link-10", LinkChange: 1, LocationCode1C: "LOC-1"},
		},
		mainReports: []MainReportRow{
			{ReportID: 101, LinkID: 10, ReportDate: "2026-06-30 08:00:00", ReportStatus: 4},
		},
		userReports: []UserReportRow{
			{
				ReportID:            101,
				ID:                  501,
				LinkID:              10,
				UserCode1C:          "EMP-1",
				ReportStatus:        4,
				PositionCode1C:      "POS-1",
				VehicleCode1C:       "",
				Val1CConfirm:        1,
				TimeTypeName:        "day",
				TimeTypeDescription: "day shift",
				MasterHours:         8,
				DispatcherHours:     8,
			},
		},
		wantReportIDs: []int{101},
	}

	service, err := NewService(storage)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	body, err := service.Export(context.Background(), Request{
		DateFrom: "2026-06-30",
		DateTo:   "2026-06-30",
	})
	if err != nil {
		t.Fatalf("export worksheets users reports: %v", err)
	}

	var response []LinkExport
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(response) != 1 {
		t.Fatalf("expected one link, got %d", len(response))
	}
	if len(response[0].MainReports) != 1 {
		t.Fatalf("expected one main report, got %d", len(response[0].MainReports))
	}
	usersInReport := response[0].MainReports[0].UsersReports
	if len(usersInReport) != 1 {
		t.Fatalf("expected user report selected by report_id, got %d", len(usersInReport))
	}
	if usersInReport[0].ReportID != 501 {
		t.Fatalf("expected exported user report id 501, got %d", usersInReport[0].ReportID)
	}
}

type exportStorageFake struct {
	links         []LinkRow
	masters       []MasterRow
	mainReports   []MainReportRow
	userReports   []UserReportRow
	wantReportIDs []int
}

func (s *exportStorageFake) GetLinks(context.Context, *string) ([]LinkRow, error) {
	return s.links, nil
}

func (s *exportStorageFake) GetMastersInLink(context.Context, *string, []int) ([]MasterRow, error) {
	return s.masters, nil
}

func (s *exportStorageFake) GetLastInactiveMastersInLink(context.Context, *string, []int) ([]MasterRow, error) {
	return nil, nil
}

func (s *exportStorageFake) GetMainReports(context.Context, string, string, *string) ([]MainReportRow, error) {
	return s.mainReports, nil
}

func (s *exportStorageFake) GetUserReports(_ context.Context, _ *string, reportIDs []int) ([]UserReportRow, error) {
	if !reflect.DeepEqual(reportIDs, s.wantReportIDs) {
		return nil, fmt.Errorf("expected reportIDs %v, got %v", s.wantReportIDs, reportIDs)
	}
	return s.userReports, nil
}

func (s *exportStorageFake) GetSheetFlagsByLocationCodes(context.Context, []string) (map[string]int, error) {
	return map[string]int{"LOC-1": 1}, nil
}
