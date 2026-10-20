package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"uuid"

	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/examples/petstore/types"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/relation"
	echo "github.com/labstack/echo/v4"
)

var serverUrlBase = "http://localhost:8081"

// Server is a echo server
type Server struct {
	db *DB
}

// NewServer creates a new server
func NewServer(db *DB) *Server {
	return &Server{db: db}
}

// Register adds the routes of the server.
func (srv *Server) Register(e *echo.Echo) {
	e.POST("/pet/:petId/uploadImage", srv.UploadImage)
	e.GET("/image/:id", srv.GetImage)
	e.POST("/pet", srv.AddPet)
	e.GET("/pet/:id", srv.GetPet)
	e.GET("/pet", srv.GetPets)
	e.DELETE("/pet/:id", srv.DeletePet)
	e.GET("/stats/categories", srv.GetCategoryStats)
	e.GET("/stats/added", srv.GetAddedStats)
}

func badRequest(format string, args ...any) error {
	return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf(format, args...).Error())
}

func (srv *Server) UploadImage(c echo.Context) error {
	petId, err := strconv.ParseInt(c.Param("petId"), 10, 64)
	if err != nil {
		return badRequest("failed to parse id: %w", err)
	}

	metadata := c.FormValue("additionalMetadata")

	file, err := c.FormFile("file")
	if err != nil {
		return badRequest("failed to get file: %w", err)
	}
	src, err := file.Open()
	if err != nil {
		return badRequest("failed to open file: %w", err)
	}
	defer src.Close()
	data, err := io.ReadAll(src)
	if err != nil {
		return badRequest("failed to read file: %w", err)
	}

	// A transaction that conflicts with another one is run again, so what is in it must be
	// safe to do twice.
	var id uuid.UUID
	err = srv.db.Update(func(tx kv.Tx) error {
		id, err = srv.db.CreateFile(tx, petId, data, metadata)
		return err
	})
	if errors.Is(err, relation.ErrMissingRecord) {
		return echo.NewHTTPError(http.StatusNotFound, "pet not found")
	}
	if err != nil {
		return fmt.Errorf("failed to upload image: %w", err)
	}

	return c.JSON(http.StatusCreated, map[string]string{"url": imageURL(id)})
}

func imageURL(id uuid.UUID) string {
	return fmt.Sprintf("%s/image/%s", serverUrlBase, id)
}

func (srv *Server) GetImage(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return badRequest("failed to parse id: %w", err)
	}

	var data []byte
	err = srv.db.View(func(tx kv.Tx) error {
		data, err = srv.db.GetFile(tx, id)
		return err
	})
	if errors.Is(err, kv.ErrNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, "image not found")
	}
	if err != nil {
		return fmt.Errorf("failed to get image: %w", err)
	}

	// Always cache the image file as it won't change
	c.Response().Header().Set("Cache-Control", "public, max-age=31536000")

	return c.Blob(http.StatusOK, http.DetectContentType(data), data)
}

func (srv *Server) AddPet(c echo.Context) error {
	var p types.Pet
	if err := c.Bind(&p); err != nil {
		return err
	}
	if p.Category == nil {
		return badRequest("category is required")
	}
	if p.Category.Id == 0 && p.Category.Name == "" {
		return badRequest("category name is required")
	}

	var id int64
	err := srv.db.Update(func(tx kv.Tx) error {
		// The category and the pet are written together, or neither is.
		catId := p.Category.Id
		if catId == 0 {
			var err error
			catId, err = srv.db.CreateCategory(tx, p.Category.Name)
			if err != nil {
				return fmt.Errorf("failed to create category: %w", err)
			}
		}

		var err error
		id, err = srv.db.CreatePet(tx, catId, p.Name, p.Status)
		return err
	})
	if errors.Is(err, relation.ErrMissingRecord) {
		return echo.NewHTTPError(http.StatusNotFound, "category not found")
	}
	if err != nil {
		return fmt.Errorf("failed to create pet: %w", err)
	}

	return c.JSON(http.StatusCreated, map[string]int64{"id": id})
}

// pet returns a pet as the API has it: with its category and the addresses of its images.
func (srv *Server) pet(tx kv.Tx, pr *types.PetRecord) (*types.Pet, error) {
	cat, err := srv.db.GetCategory(tx, pr.CategoryId)
	if err != nil {
		return nil, fmt.Errorf("failed to get category: %w", err)
	}

	p := &types.Pet{
		Id:        pr.Id,
		Category:  cat,
		Name:      pr.Name,
		Status:    pr.Status,
		CreatedAt: pr.CreatedAt,
	}

	images, err := srv.db.GetPetImageIds(tx, pr.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to get pet images: %w", err)
	}
	for _, id := range images {
		p.PhotoUrls = append(p.PhotoUrls, imageURL(id))
	}
	return p, nil
}

func (srv *Server) GetPet(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return badRequest("failed to parse id: %w", err)
	}

	var p *types.Pet
	err = srv.db.View(func(tx kv.Tx) error {
		pr, err := srv.db.GetPet(tx, id)
		if err != nil {
			return err
		}
		p, err = srv.pet(tx, pr)
		return err
	})
	if errors.Is(err, kv.ErrNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, "pet not found")
	}
	if err != nil {
		return fmt.Errorf("failed to get pet: %w", err)
	}

	return c.JSON(http.StatusOK, p)
}

// intParam returns a parameter of the query that is a number, or a default if it is not given.
func intParam(c echo.Context, name string, def int) (int, error) {
	text := c.QueryParam(name)
	if text == "" {
		return def, nil
	}
	n, err := strconv.Atoi(text)
	if err != nil || n < 0 {
		return 0, badRequest("failed to parse %s: %q is not a number that is zero or more", name, text)
	}
	return n, nil
}

func (srv *Server) GetPets(c echo.Context) error {
	offset, err := intParam(c, "offset", 0)
	if err != nil {
		return err
	}
	limit, err := intParam(c, "limit", 20)
	if err != nil {
		return err
	}

	// The pets of a status are found by the index of the statuses.
	var status *types.Status
	if text := c.QueryParam("status"); text != "" {
		value, ok := types.Status_value[strings.ToUpper(text)]
		if !ok {
			return badRequest("unknown status %q", text)
		}
		status = new(types.Status(value))
	}

	pets := []*types.Pet{}
	err = srv.db.View(func(tx kv.Tx) error {
		petRecords, err := srv.db.ListPets(tx, status, limit, offset)
		if err != nil {
			return err
		}
		for _, pr := range petRecords {
			p, err := srv.pet(tx, pr)
			if err != nil {
				return err
			}
			pets = append(pets, p)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to list pets: %w", err)
	}

	return c.JSON(http.StatusOK, pets)
}

func (srv *Server) DeletePet(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return badRequest("failed to parse id: %w", err)
	}

	err = srv.db.Update(func(tx kv.Tx) error {
		return srv.db.DeletePet(tx, id)
	})
	if err != nil {
		return fmt.Errorf("failed to delete pet: %w", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// GetCategoryStats returns the number of the pets of each category.
func (srv *Server) GetCategoryStats(c echo.Context) error {
	counts := []CategoryCount{}
	err := srv.db.View(func(tx kv.Tx) error {
		var err error
		counts, err = srv.db.CountPetsByCategory(tx)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to count pets: %w", err)
	}

	return c.JSON(http.StatusOK, counts)
}

// AddedCount is the number of the pets that were added in a span of time.
type AddedCount struct {
	Start string `json:"start"`
	Pets  int64  `json:"pets"`
}

const dateLayout = "2006-01-02"

// GetAddedStats returns the number of the pets that were added in each day, week or month
// from a date to another one, which are the last 30 days if they are not given.
func (srv *Server) GetAddedStats(c echo.Context) error {
	var bucket agg.Bucket
	switch by := c.QueryParam("by"); by {
	case "", "day":
		bucket = agg.Daily
	case "week":
		bucket = agg.Weekly
	case "month":
		bucket = agg.Monthly
	default:
		return badRequest("unknown span %q: it is day, week or month", by)
	}

	to := time.Now().UTC()
	from := to.AddDate(0, 0, -30)
	var err error
	if text := c.QueryParam("from"); text != "" {
		if from, err = time.Parse(dateLayout, text); err != nil {
			return badRequest("failed to parse from: %w", err)
		}
	}
	if text := c.QueryParam("to"); text != "" {
		if to, err = time.Parse(dateLayout, text); err != nil {
			return badRequest("failed to parse to: %w", err)
		}
		// The day that is given is in the range.
		to = to.AddDate(0, 0, 1)
	}

	counts := []AddedCount{}
	err = srv.db.View(func(tx kv.Tx) error {
		points, err := srv.db.PetsAdded(tx, bucket, from, to)
		if err != nil {
			return err
		}
		for _, p := range points {
			counts = append(counts, AddedCount{Start: p.Start.Format(dateLayout), Pets: p.State})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to count pets: %w", err)
	}

	return c.JSON(http.StatusOK, counts)
}
