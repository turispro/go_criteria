package go_criteria

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"

	sq "github.com/elgris/sqrl"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	godotenv.Load()
}

var operatorsMap = map[string]string{
	"eq":       "=",
	"gt":       ">",
	"gte":      ">=",
	"lt":       "<",
	"lte":      "<=",
	"contains": "like",
}

type Criteria struct {
	Filters    []Filter
	Limit      int
	Offset     int
	Order      Order
	Select     string
	From       string
	JoinClause []string
	FieldMap   map[string]string
	Hydrators  map[string]func(value any) any
	GroupBy    string
}

type Order struct {
	Field string
	Type  string
}

type Filter struct {
	Field    string
	Operator string
	Value    interface{}
}

func MustFromUrl(url *url.URL) *Criteria {
	criteria, err := FromURL(url)
	if err != nil {
		return &Criteria{}
	}
	return criteria
}
func FromURL(url *url.URL) (*Criteria, error) {
	query := url.Query()
	if len(query) == 0 {
		return &Criteria{}, errors.New("url query not present")
	}
	var filters []Filter
	var order Order
	criteria := Criteria{}
	for field, value := range query {
		if field == "limit" {
			limit, _ := strconv.Atoi(value[0])
			criteria.Limit = limit
			continue
		}

		if field == "offset" {
			offset, _ := strconv.Atoi(value[0])
			criteria.Offset = offset
			continue
		}
		field, op, err := extractFieldOperatorFromField(field)
		if err != nil {
			return nil, err
		}

		if field == "order" {
			order = Order{Field: value[0], Type: op}
			continue
		}

		for _, filterValue := range value {

			filters = append(filters, Filter{field, op, filterValue})
		}

	}
	criteria.Filters = filters
	criteria.Order = order
	return &criteria, nil
}

func (c *Criteria) ToSql() (string, []interface{}, error) {
	query := sq.Select(c.Select).
		From(c.From)
	for _, join := range c.JoinClause {
		query.JoinClause(join)
	}

	if c.GroupBy != "" {
		query.GroupBy(c.GroupBy)
	}

	if c.Limit > 0 {
		query.Limit(uint64(c.Limit))
	}

	if c.Offset > 0 {
		query.Offset(uint64(c.Offset))
	}

	if c.Order.Field != "" {
		field := mappedField(c.FieldMap, c.Order.Field)
		query.OrderBy(fmt.Sprintf("%s %s", field, c.Order.Type))
	}

	for _, filter := range c.Filters {
		field := mappedField(c.FieldMap, filter.Field)
		if operator, ok := operatorsMap[filter.Operator]; ok {
			var value any
			if fun, ok := c.Hydrators[filter.Field]; ok {
				value = fun(filter.Value)
			} else {
				value = filter.Value
			}
			if operator == "like" {
				query.Where(fmt.Sprintf("%s LIKE ?", field), fmt.Sprintf("%%%s%%", value))
			} else {
				query.Where(fmt.Sprintf("%s %s ?", field, operator), value)
			}
		}

	}

	return query.ToSql()
}

func (c *Criteria) ToBson() (bson.D, *options.FindOptions) {
	opts := options.Find()
	if c.Order.Field != "" {
		var sort int
		if c.Order.Type == "asc" {
			sort = 1
		} else {
			sort = -1
		}
		opts.SetSort(bson.D{{c.Order.Field, sort}})
	}

	if c.Limit > 0 {
		opts.SetLimit(int64(c.Limit))
	}

	if c.Offset > 0 {
		opts.SetSkip(int64(c.Offset))
	}

	filter := bson.D{}

	for _, f := range c.Filters {
		var value any
		if valueFunc, ok := c.Hydrators[f.Field]; ok {
			value = valueFunc(f.Value)
		} else {
			value = f.Value
		}

		var field string
		if fieldValue, ok := c.FieldMap[f.Field]; ok {
			field = fieldValue
		} else {
			field = f.Field
		}
		if f.Operator == "like" {
			filter = append(filter, bson.E{field, bson.M{"$regex": fmt.Sprintf(".*%s.*", value), "$options": "i"}})
		} else {
			operator := getMongoOperator(f.Operator)
			filter = append(filter, bson.E{field, bson.D{{operator, value}}})
		}
	}
	return filter, opts
}

func getMongoOperator(operator string) string {
	return fmt.Sprintf("$%s", operator)
}

func mappedField(fildMap map[string]string, fieldToMap string) (field string) {

	if f, ok := fildMap[fieldToMap]; ok {
		field = f
	} else {
		field = fieldToMap
	}
	return
}
func extractFieldOperatorFromField(field string) (string, string, error) {

	r, _ := regexp.Compile(`([a-z_]+)\[([a-z=]+)\]`)
	op := r.FindStringSubmatch(field)
	if len(op) < 3 {
		return "", "", errors.New("operator not found")
	}
	return op[1], op[2], nil
}
