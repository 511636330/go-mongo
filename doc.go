package mongo

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Document struct {
	Id        primitive.ObjectID `bson:"_id,omitempty"`
	CreatedAt time.Time          `bson:"created_at,omitempty"`
	UpdatedAt time.Time          `bson:"updated_at,omitempty"`
	DeletedAt time.Time          `bson:"deleted_at,omitempty"`
}

func (m *Document) GetId() primitive.ObjectID {
	return m.Id
}

func (m *Document) SetId(id primitive.ObjectID) {
	m.Id = id
}

func (m *Document) GetCreatedAt() time.Time {
	return m.CreatedAt
}

func (m *Document) SetCreatedAt(t time.Time) {
	m.CreatedAt = t
}

func (m *Document) GetUpdatedAt() time.Time {
	return m.UpdatedAt
}

func (m *Document) SetUpdatedAt(t time.Time) {
	m.UpdatedAt = t
}

func (m *Document) GetDeletedAt() time.Time {
	return m.DeletedAt
}

func (m *Document) SetDeletedAt(t time.Time) {
	m.UpdatedAt = t
}
