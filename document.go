package main

import (
	set "github.com/deckarep/golang-set"
)

type Metadatum struct {
	Attribute string `json:"attribute"`
	Value     string `json:"value"`
	Unit      string `json:"unit"`
}

type UserPermission struct {
	User       string `json:"user"`
	Permission string `json:"permission"`
}

type ElasticsearchDocument struct {
	Id              string           `json:"id"`
	Path            string           `json:"path"`
	Label           string           `json:"label"`
	Creator         string           `json:"creator"`
	FileType        string           `json:"fileType"`
	DateCreated     int64            `json:"dateCreated"`
	DateModified    int64            `json:"dateModified"`
	FileSize        int64            `json:"fileSize"`
	Metadata        []Metadatum      `json:"metadata"`
	UserPermissions []UserPermission `json:"userPermissions"`
}

func metadataEqual(one, two []Metadatum) bool {
	om := make([]interface{}, len(one))
	for i := range one {
		om[i] = one[i]
	}
	tm := make([]interface{}, len(two))
	for i := range two {
		tm[i] = two[i]
	}
	if !set.NewSetFromSlice(om).Equal(set.NewSetFromSlice(tm)) {
		return false
	}
	return true
}

func permsEqual(one, two []UserPermission) bool {
	om := make([]interface{}, len(one))
	for i := range one {
		om[i] = one[i]
	}
	tm := make([]interface{}, len(two))
	for i := range two {
		tm[i] = two[i]
	}
	if !set.NewSetFromSlice(om).Equal(set.NewSetFromSlice(tm)) {
		return false
	}
	return true
}

func (doc ElasticsearchDocument) Equal(other ElasticsearchDocument) bool {
	// User-modifiable fields in rough "likelihood" order
	if doc.DateModified != other.DateModified {
		return false
	}
	if doc.FileSize != other.FileSize {
		return false
	}
	if doc.Path != other.Path {
		return false
	}
	if doc.Label != other.Label {
		return false
	}

	// Fields which shouldn't change for the same object
	if doc.Id != other.Id {
		return false
	}
	if doc.Creator != other.Creator {
		return false
	}
	if doc.FileType != other.FileType {
		return false
	}
	if doc.DateCreated != other.DateCreated {
		return false
	}

	// More computationally intensive fields to compare
	if !metadataEqual(doc.Metadata, other.Metadata) {
		return false
	}

	if !permsEqual(doc.UserPermissions, other.UserPermissions) {
		return false
	}

	return true
}