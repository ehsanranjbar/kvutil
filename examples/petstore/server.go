package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/examples/petstore/types"
	"github.com/google/uuid"
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

func (srv *Server) UploadImage(c echo.Context) error {
	petId, err := strconv.ParseInt(c.Param("petId"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse id: %w", err))
	}

	metadata := c.FormValue("additionalMetadata")

	file, err := c.FormFile("file")
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to get file: %w", err))
	}
	src, err := file.Open()
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to open file: %w", err))
	}
	defer src.Close()
	data, err := io.ReadAll(src)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to read file: %w", err))
	}

	txn := srv.db.base.NewTransaction(true)
	defer txn.Discard()

	_, err = srv.db.CreateFile(txn, petId, data, metadata)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to upload image: %w", err))
	}

	err = txn.Commit()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to commit transaction: %w", err))
	}

	return c.NoContent(http.StatusCreated)
}

func (srv *Server) GetImage(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse id: %w", err))
	}

	txn := srv.db.base.NewTransaction(false)
	defer txn.Discard()

	data, err := srv.db.GetFile(txn, id)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Errorf("image not found"))
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to get image: %w", err))
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

	txn := srv.db.base.NewTransaction(true)
	defer txn.Discard()

	if p.Category == nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("category is required"))
	}
	catId := p.Category.Id
	if p.Category.Id == 0 {
		if p.Category.Name == "" {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("category name is required"))
		}

		var err error
		catId, err = srv.db.CreateCategory(txn, p.Category.Name)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to create category: %w", err))
		}
		p.Category.Id = catId
	}

	_, err := srv.db.CreatePet(txn, catId, p.Name, p.Status)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to create pet: %w", err))
	}

	err = txn.Commit()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to commit transaction: %w", err))
	}

	return c.NoContent(http.StatusCreated)
}

func (srv *Server) GetPet(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse id: %w", err))
	}

	txn := srv.db.base.NewTransaction(false)
	defer txn.Discard()

	pr, err := srv.db.GetPet(txn, id)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Errorf("pet not found"))
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to get pet: %w", err))
	}

	cat, err := srv.db.GetCategory(txn, pr.CategoryId)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to get category: %w", err))
	}

	p := &types.Pet{
		Id:       pr.Id,
		Category: cat,
		Name:     pr.Name,
		Status:   pr.Status,
	}

	images, err := srv.db.GetPetImageIds(txn, pr.Id)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to get pet images: %w", err))
	}
	for _, id := range images {
		p.PhotoUrls = append(p.PhotoUrls, fmt.Sprintf("%s/image/%s", serverUrlBase, id))
	}

	return c.JSON(http.StatusOK, p)
}

func (srv *Server) GetPets(c echo.Context) error {
	offset, err := strconv.ParseInt(c.QueryParam("offset"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse offset: %w", err))
	}
	limit, err := strconv.ParseInt(c.QueryParam("limit"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse limit: %w", err))
	}

	txn := srv.db.base.NewTransaction(false)
	defer txn.Discard()

	petRecords, err := srv.db.ListPets(txn, int(limit), int(offset))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to list pets: %w", err))
	}

	var pets []*types.Pet
	for _, pr := range petRecords {
		cat, err := srv.db.GetCategory(txn, pr.CategoryId)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to get category: %w", err))
		}

		p := &types.Pet{
			Id:       pr.Id,
			Category: cat,
			Name:     pr.Name,
			Status:   pr.Status,
		}

		images, err := srv.db.GetPetImageIds(txn, pr.Id)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to get pet images: %w", err))
		}

		for _, id := range images {
			p.PhotoUrls = append(p.PhotoUrls, fmt.Sprintf("%s/image/%s", serverUrlBase, id))
		}
		pets = append(pets, p)
	}

	return c.JSON(http.StatusOK, pets)
}

func (srv *Server) DeletePet(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse id: %w", err))
	}

	txn := srv.db.base.NewTransaction(true)
	defer txn.Discard()

	err = srv.db.DeletePet(txn, id)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to delete pet: %w", err))
	}

	return c.NoContent(http.StatusNoContent)
}
