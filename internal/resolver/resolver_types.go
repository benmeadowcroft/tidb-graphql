package resolver

import (
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/scalars"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

func firstFieldAST(fields []*ast.Field) *ast.Field {
	if len(fields) == 0 {
		return nil
	}
	return fields[0]
}

func (r *Resolver) orderByArgType(table introspection.Table) graphql.Input {
	clauseInput := r.orderByClauseInput(table)
	if clauseInput == nil {
		return nil
	}
	return graphql.NewList(graphql.NewNonNull(clauseInput))
}

func (r *Resolver) orderByClauseInput(table introspection.Table) *graphql.InputObject {
	fields := planner.OrderByIndexedFields(table)
	if len(fields) == 0 {
		return nil
	}
	typeName := introspection.GraphQLTypeName(table) + "OrderByClauseInput"
	r.mu.RLock()
	cached, ok := r.orderByClauseCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	orderDirection := r.orderDirectionEnum()
	clauseFields := graphql.InputObjectConfigFieldMap{}
	for _, name := range sortedOrderByFieldNames(fields) {
		clauseFields[name] = &graphql.InputObjectFieldConfig{
			Type: orderDirection,
		}
	}

	input := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   typeName,
		Fields: clauseFields,
	})
	r.mu.Lock()
	if cached, ok := r.orderByClauseCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.orderByClauseCache[typeName] = input
	r.mu.Unlock()

	return input
}

func (r *Resolver) orderDirectionEnum() *graphql.Enum {
	r.mu.RLock()
	cached := r.orderDirection
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	enumValue := graphql.NewEnum(graphql.EnumConfig{
		Name: "OrderDirection",
		Values: graphql.EnumValueConfigMap{
			"ASC":  &graphql.EnumValueConfig{Value: "ASC"},
			"DESC": &graphql.EnumValueConfig{Value: "DESC"},
		},
	})

	r.mu.Lock()
	if r.orderDirection == nil {
		r.orderDirection = enumValue
	}
	cached = r.orderDirection
	r.mu.Unlock()

	return cached
}

func (r *Resolver) orderByPolicyEnum() *graphql.Enum {
	r.mu.RLock()
	cached := r.orderByPolicy
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	enumValue := graphql.NewEnum(graphql.EnumConfig{
		Name: "OrderByPolicy",
		Values: graphql.EnumValueConfigMap{
			string(planner.OrderByPolicyIndexPrefixOnly): &graphql.EnumValueConfig{Value: string(planner.OrderByPolicyIndexPrefixOnly)},
			string(planner.OrderByPolicyAllowNonPrefix):  &graphql.EnumValueConfig{Value: string(planner.OrderByPolicyAllowNonPrefix)},
		},
	})

	r.mu.Lock()
	if r.orderByPolicy == nil {
		r.orderByPolicy = enumValue
	}
	cached = r.orderByPolicy
	r.mu.Unlock()

	return cached
}

func (r *Resolver) nonNegativeIntScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.nonNegativeInt
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.NonNegativeInt()

	r.mu.Lock()
	if r.nonNegativeInt == nil {
		r.nonNegativeInt = scalar
	}
	cached = r.nonNegativeInt
	r.mu.Unlock()

	return cached
}

func (r *Resolver) jsonScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.jsonType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.JSON()

	r.mu.Lock()
	if r.jsonType == nil {
		r.jsonType = scalar
	}
	cached = r.jsonType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) bigIntScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.bigIntType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.BigInt()

	r.mu.Lock()
	if r.bigIntType == nil {
		r.bigIntType = scalar
	}
	cached = r.bigIntType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) decimalScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.decimalType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Decimal()

	r.mu.Lock()
	if r.decimalType == nil {
		r.decimalType = scalar
	}
	cached = r.decimalType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) dateScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.dateType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Date()

	r.mu.Lock()
	if r.dateType == nil {
		r.dateType = scalar
	}
	cached = r.dateType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) timeScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.timeType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Time()

	r.mu.Lock()
	if r.timeType == nil {
		r.timeType = scalar
	}
	cached = r.timeType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) yearScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.yearType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Year()

	r.mu.Lock()
	if r.yearType == nil {
		r.yearType = scalar
	}
	cached = r.yearType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) bytesScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.bytesType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Bytes()

	r.mu.Lock()
	if r.bytesType == nil {
		r.bytesType = scalar
	}
	cached = r.bytesType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) uuidScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.uuidType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.UUID()

	r.mu.Lock()
	if r.uuidType == nil {
		r.uuidType = scalar
	}
	cached = r.uuidType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) vectorScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.vectorType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Vector()

	r.mu.Lock()
	if r.vectorType == nil {
		r.vectorType = scalar
	}
	cached = r.vectorType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) vectorDistanceMetricEnum() *graphql.Enum {
	r.mu.RLock()
	cached := r.vectorDistance
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	enumValue := graphql.NewEnum(graphql.EnumConfig{
		Name: "VectorDistanceMetric",
		Values: graphql.EnumValueConfigMap{
			string(planner.VectorDistanceMetricCosine): &graphql.EnumValueConfig{Value: string(planner.VectorDistanceMetricCosine)},
			string(planner.VectorDistanceMetricL2):     &graphql.EnumValueConfig{Value: string(planner.VectorDistanceMetricL2)},
		},
	})

	r.mu.Lock()
	if r.vectorDistance == nil {
		r.vectorDistance = enumValue
	}
	cached = r.vectorDistance
	r.mu.Unlock()

	return cached
}

func (r *Resolver) nodeInterfaceType() *graphql.Interface {
	r.mu.RLock()
	cached := r.nodeInterface
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	nodeInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "Node",
		Fields: graphql.Fields{
			"id": &graphql.Field{Type: graphql.NewNonNull(graphql.ID)},
		},
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			source, ok := p.Value.(map[string]interface{})
			if !ok {
				return nil
			}
			typeName, ok := source["__typename"].(string)
			if !ok || typeName == "" {
				return nil
			}
			r.mu.RLock()
			objType := r.typeCache[typeName]
			r.mu.RUnlock()
			return objType
		},
	})

	r.mu.Lock()
	if r.nodeInterface == nil {
		r.nodeInterface = nodeInterface
	}
	cached = r.nodeInterface
	r.mu.Unlock()

	return cached
}

// pageInfoType returns the shared PageInfo GraphQL type (lazy-init).
func (r *Resolver) getPageInfoType() *graphql.Object {
	r.mu.RLock()
	cached := r.pageInfoType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	pageInfo := graphql.NewObject(graphql.ObjectConfig{
		Name: "PageInfo",
		Fields: graphql.Fields{
			"hasNextPage": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Boolean),
			},
			"hasPreviousPage": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Boolean),
			},
			"startCursor": &graphql.Field{
				Type: graphql.String,
			},
			"endCursor": &graphql.Field{
				Type: graphql.String,
			},
		},
	})

	r.mu.Lock()
	if r.pageInfoType == nil {
		r.pageInfoType = pageInfo
	}
	cached = r.pageInfoType
	r.mu.Unlock()

	return cached
}

// buildEdgeType builds the Edge type for a table (cached per table).
func (r *Resolver) buildEdgeType(table introspection.Table, tableType *graphql.Object) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + "Edge"

	r.mu.RLock()
	if cached, ok := r.edgeCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	edgeType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.Fields{
			"cursor": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
			"node": &graphql.Field{
				Type: graphql.NewNonNull(tableType),
			},
		},
	})

	r.mu.Lock()
	if cached, ok := r.edgeCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.edgeCache[typeName] = edgeType
	r.mu.Unlock()

	return edgeType
}

// buildConnectionType builds the Connection type for a table (cached per table).
func (r *Resolver) buildConnectionType(table introspection.Table, tableType *graphql.Object) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + "Connection"

	r.mu.RLock()
	if cached, ok := r.connectionCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	edgeType := r.buildEdgeType(table, tableType)
	pageInfo := r.getPageInfoType()
	aggregateType := r.buildAggregateFieldsType(table)

	connType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.Fields{
			"edges": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(edgeType))),
			},
			"nodes": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(tableType))),
			},
			"pageInfo": &graphql.Field{
				Type: graphql.NewNonNull(pageInfo),
			},
			"totalCount": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Int),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					source, ok := p.Source.(map[string]interface{})
					if !ok {
						return 0, nil
					}
					cr, ok := source["__connectionResult"].(*connectionResult)
					if !ok || cr == nil || cr.plan == nil {
						return 0, nil
					}
					return cr.totalCount()
				},
			},
			"aggregate": &graphql.Field{
				Type: graphql.NewNonNull(aggregateType),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					source, ok := p.Source.(map[string]interface{})
					if !ok {
						return map[string]interface{}{"count": 0}, nil
					}
					cr, ok := source["__connectionResult"].(*connectionResult)
					if !ok || cr == nil {
						return map[string]interface{}{"count": 0}, nil
					}
					field := firstFieldAST(p.Info.FieldASTs)
					selection := planner.ParseAggregateSelection(table, field, p.Info.Fragments)
					return cr.aggregate(selection)
				},
			},
		},
	})

	r.mu.Lock()
	if cached, ok := r.connectionCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.connectionCache[typeName] = connType
	r.mu.Unlock()

	return connType
}

func (r *Resolver) vectorTypeSuffix(vectorCol introspection.Column) string {
	return r.singularNamer.ToGraphQLTypeName(introspection.GraphQLFieldName(vectorCol)) + "Vector"
}

func (r *Resolver) buildVectorEdgeType(table introspection.Table, vectorCol introspection.Column, tableType *graphql.Object) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + r.vectorTypeSuffix(vectorCol) + "Edge"

	r.mu.RLock()
	if cached, ok := r.vectorEdgeCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	edgeType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.Fields{
			"cursor": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
			"node": &graphql.Field{
				Type: graphql.NewNonNull(tableType),
			},
			"distance": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Float),
			},
			"rank": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Int),
			},
		},
	})

	r.mu.Lock()
	if cached, ok := r.vectorEdgeCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.vectorEdgeCache[typeName] = edgeType
	r.mu.Unlock()

	return edgeType
}

func (r *Resolver) buildVectorConnectionType(table introspection.Table, vectorCol introspection.Column, tableType *graphql.Object) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + r.vectorTypeSuffix(vectorCol) + "Connection"

	r.mu.RLock()
	if cached, ok := r.vectorConnCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	edgeType := r.buildVectorEdgeType(table, vectorCol, tableType)
	pageInfo := r.getPageInfoType()

	connType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.Fields{
			"edges": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(edgeType))),
			},
			"nodes": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(tableType))),
			},
			"pageInfo": &graphql.Field{
				Type: graphql.NewNonNull(pageInfo),
			},
		},
	})

	r.mu.Lock()
	if cached, ok := r.vectorConnCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.vectorConnCache[typeName] = connType
	r.mu.Unlock()

	return connType
}
