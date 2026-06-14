package usersreports

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
)

type Exporter interface {
	Export(ctx context.Context, req Request) ([]byte, error)
}

type ExportStorage interface {
	GetLinks(ctx context.Context, locationCode1C *string) ([]LinkRow, error)
	GetMastersInLink(ctx context.Context, locationCode1C *string, linkIDs []int) ([]MasterRow, error)
	GetLastInactiveMastersInLink(ctx context.Context, locationCode1C *string, linkIDs []int) ([]MasterRow, error)
	GetMainReports(ctx context.Context, dateFrom string, dateTo string, locationCode1C *string) ([]MainReportRow, error)
	GetUserReports(ctx context.Context, employeeCode1C *string, reportIDs []int) ([]UserReportRow, error)
	GetSheetFlagsByLocationCodes(ctx context.Context, locationCodes1C []string) (map[string]int, error)
}

type Service struct {
	storage ExportStorage
}

func NewService(storage ExportStorage) (*Service, error) {
	if storage == nil {
		return nil, fmt.Errorf("export storage is required")
	}

	return &Service{storage: storage}, nil
}

func (s *Service) Export(ctx context.Context, req Request) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("users reports service is nil")
	}

	links, err := s.storage.GetLinks(ctx, req.LocationCode1C)
	if err != nil {
		return nil, fmt.Errorf("load links: %w", err)
	}
	if len(links) == 0 {
		return []byte("[]"), nil
	}

	linkIDs := make([]int, 0, len(links))
	locationCodes := make([]string, 0, len(links))
	for _, link := range links {
		linkIDs = append(linkIDs, link.LinkID)
		if link.LocationCode1C != "" {
			locationCodes = append(locationCodes, link.LocationCode1C)
		}
	}

	masters, err := s.storage.GetMastersInLink(ctx, req.LocationCode1C, linkIDs)
	if err != nil {
		return nil, fmt.Errorf("load masters: %w", err)
	}

	mainReports, err := s.storage.GetMainReports(ctx, req.DateFrom, req.DateTo, req.LocationCode1C)
	if err != nil {
		return nil, fmt.Errorf("load main reports: %w", err)
	}

	reportIDs := mainReportIDs(mainReports)
	userReports := make([]UserReportRow, 0)
	if len(reportIDs) > 0 {
		userReports, err = s.storage.GetUserReports(ctx, req.EmployeeCode1C, reportIDs)
		if err != nil {
			return nil, fmt.Errorf("load user reports: %w", err)
		}
	}

	mastersByLink := buildMastersByLink(masters)
	missingLinkIDs := missingMasterLinkIDs(links, mastersByLink)
	if len(missingLinkIDs) > 0 {
		fallbackMasters, err := s.storage.GetLastInactiveMastersInLink(ctx, req.LocationCode1C, missingLinkIDs)
		if err != nil {
			return nil, fmt.Errorf("load fallback masters: %w", err)
		}

		for _, master := range fallbackMasters {
			if len(mastersByLink[master.LinkID]) > 0 {
				continue
			}

			mastersByLink[master.LinkID] = []Master{
				{
					UserCode1C:  master.UserCode1C,
					MasterIndex: master.MasterIndex,
				},
			}
		}
	}

	sheetFlagsByLocation, err := s.storage.GetSheetFlagsByLocationCodes(ctx, uniqueNonEmptyStrings(locationCodes))
	if err != nil {
		return nil, fmt.Errorf("load sheet flags: %w", err)
	}

	reportsByLink := make(map[int][]MainReportRow, len(mainReports))
	for _, report := range mainReports {
		reportsByLink[report.LinkID] = append(reportsByLink[report.LinkID], report)
	}

	userReportsByReport := buildUserReportsByReport(userReports)
	response := make([]LinkExport, 0, len(links))

	for _, link := range links {
		linkReports := reportsByLink[link.LinkID]
		if len(linkReports) == 0 {
			continue
		}

		mainReportExports := make([]MainReport, 0, len(linkReports))
		for _, report := range linkReports {
			usersInReport := userReportsByReport[report.ReportID]
			if usersInReport == nil {
				usersInReport = make([]UserReport, 0)
			}

			mainReportExports = append(mainReportExports, MainReport{
				ReportID:             report.ReportID,
				ReportDate:           report.ReportDate,
				ReportStatus:         report.ReportStatus,
				ReportLocationCode1C: "",
				UsersReports:         usersInReport,
			})
		}

		masters := mastersByLink[link.LinkID]
		if masters == nil {
			masters = make([]Master, 0)
		}

		response = append(response, LinkExport{
			LinkID:         link.LinkID,
			LinkName:       link.LinkName,
			ChangeID:       link.LinkChange,
			LocationCode1C: link.LocationCode1C,
			IsSheet:        sheetFlagsByLocation[link.LocationCode1C],
			Masters:        masters,
			MainReports:    mainReportExports,
		})
	}

	body, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("marshal worksheets users reports export: %w", err)
	}

	return body, nil
}

type LinkRow struct {
	LinkID         int
	LinkName       string
	LinkChange     int
	LocationCode1C string
}

type MasterRow struct {
	UserCode1C  string
	LinkID      int
	MasterIndex int
}

type MainReportRow struct {
	ReportID     int
	LinkID       int
	ReportDate   string
	ReportStatus int
}

type UserReportRow struct {
	ReportID            int
	ID                  int
	LinkID              int
	UserCode1C          string
	ReportStatus        int
	PositionCode1C      string
	VehicleCode1C       string
	Val1CConfirm        int
	TimeTypeName        string
	TimeTypeDescription string
	MasterHours         int
	DispatcherHours     int
}

type LinkExport struct {
	LinkID         int          `json:"link_id"`
	LinkName       string       `json:"link_name"`
	ChangeID       int          `json:"change_id"`
	LocationCode1C string       `json:"location_code_1c"`
	IsSheet        int          `json:"is_sheet"`
	Masters        []Master     `json:"masters"`
	MainReports    []MainReport `json:"main_reports"`
}

type Master struct {
	UserCode1C  string `json:"user_code_1c"`
	MasterIndex int    `json:"master_index"`
}

type MainReport struct {
	ReportID             int          `json:"report_id"`
	ReportDate           string       `json:"report_date"`
	ReportStatus         int          `json:"report_status"`
	ReportLocationCode1C string       `json:"report_location_code_1c"`
	UsersReports         []UserReport `json:"users_reports"`
}

type UserReport struct {
	ReportID       int             `json:"report_id"`
	UserCode1C     string          `json:"user_code_1c"`
	ReportStatus   int             `json:"report_status"`
	PositionCode1C string          `json:"position_code_1c"`
	VehicleCode1C  string          `json:"vehicle_code_1c"`
	Val1CConfirm   int             `json:"val_1c_confirm"`
	WorkTime       []WorkTimeEntry `json:"work_time"`
	Hours          []HoursEntry    `json:"hours"`
}

type WorkTimeEntry struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type HoursEntry struct {
	MasterHours     int `json:"master_hours"`
	DispatcherHours int `json:"dispatcher_hours"`
}

func buildMastersByLink(rows []MasterRow) map[int][]Master {
	mastersByLink := make(map[int][]Master, len(rows))
	for _, row := range rows {
		mastersByLink[row.LinkID] = append(mastersByLink[row.LinkID], Master{
			UserCode1C:  row.UserCode1C,
			MasterIndex: row.MasterIndex,
		})
	}

	for linkID, masters := range mastersByLink {
		slices.SortFunc(masters, func(left Master, right Master) int {
			return left.MasterIndex - right.MasterIndex
		})
		mastersByLink[linkID] = masters
	}

	return mastersByLink
}

func missingMasterLinkIDs(links []LinkRow, mastersByLink map[int][]Master) []int {
	missing := make([]int, 0)
	for _, link := range links {
		if link.LinkID <= 0 {
			continue
		}
		if len(mastersByLink[link.LinkID]) == 0 {
			missing = append(missing, link.LinkID)
		}
	}
	return missing
}

func buildUserReportsByReport(rows []UserReportRow) map[int][]UserReport {
	userReportsByReport := make(map[int][]UserReport, len(rows))
	for _, row := range rows {
		userReportsByReport[row.ReportID] = append(userReportsByReport[row.ReportID], UserReport{
			ReportID:       row.ID,
			UserCode1C:     row.UserCode1C,
			ReportStatus:   row.ReportStatus,
			PositionCode1C: row.PositionCode1C,
			VehicleCode1C:  row.VehicleCode1C,
			Val1CConfirm:   row.Val1CConfirm,
			WorkTime: []WorkTimeEntry{
				{
					Name:        row.TimeTypeName,
					Description: row.TimeTypeDescription,
				},
			},
			Hours: []HoursEntry{
				{
					MasterHours:     row.MasterHours,
					DispatcherHours: row.DispatcherHours,
				},
			},
		})
	}

	return userReportsByReport
}

func mainReportIDs(rows []MainReportRow) []int {
	seen := make(map[int]struct{}, len(rows))
	result := make([]int, 0, len(rows))
	for _, row := range rows {
		if row.ReportID <= 0 {
			continue
		}
		if _, ok := seen[row.ReportID]; ok {
			continue
		}
		seen[row.ReportID] = struct{}{}
		result = append(result, row.ReportID)
	}
	return result
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
