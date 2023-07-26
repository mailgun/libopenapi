// Copyright 2022 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package v3

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/pb33f/libopenapi/datamodel"
	"github.com/pb33f/libopenapi/datamodel/low"
	"github.com/pb33f/libopenapi/index"
	"github.com/pb33f/libopenapi/utils"
	"gopkg.in/yaml.v3"
)

// Paths represents a high-level OpenAPI 3+ Paths object, that is backed by a low-level one.
//
// Holds the relative paths to the individual endpoints and their operations. The path is appended to the URL from the
// Server Object in order to construct the full URL. The Paths MAY be empty, due to Access Control List (ACL)
// constraints.
//   - https://spec.openapis.org/oas/v3.1.0#paths-object
type Paths struct {
	PathItems  map[low.KeyReference[string]]low.ValueReference[*PathItem]
	Extensions map[low.KeyReference[string]]low.ValueReference[any]
	*low.Reference
}

// FindPath will attempt to locate a PathItem using the provided path string.
func (p *Paths) FindPath(path string) *low.ValueReference[*PathItem] {
	for k, j := range p.PathItems {
		if k.Value == path {
			return &j
		}
	}
	return nil
}

// FindPathAndKey attempts to locate a PathItem instance, given a path key.
func (p *Paths) FindPathAndKey(path string) (*low.KeyReference[string], *low.ValueReference[*PathItem]) {
	for k, j := range p.PathItems {
		if k.Value == path {
			return &k, &j
		}
	}
	return nil, nil
}

// FindExtension will attempt to locate an extension using the specified string.
func (p *Paths) FindExtension(ext string) *low.ValueReference[any] {
	return low.FindItemInMap[any](ext, p.Extensions)
}

// GetExtensions returns all Paths extensions and satisfies the low.HasExtensions interface.
func (p *Paths) GetExtensions() map[low.KeyReference[string]]low.ValueReference[any] {
	return p.Extensions
}

// Build will extract extensions and all PathItems. This happens asynchronously for speed.
func (p *Paths) Build(root *yaml.Node, idx *index.SpecIndex) error {
	root = utils.NodeAlias(root)
	utils.CheckForMergeNodes(root)
	p.Reference = new(low.Reference)
	p.Extensions = low.ExtractExtensions(root)

	// Translate YAML nodes to pathsMap using `TranslatePipeline`.
	type pathBuildResult struct {
		k low.KeyReference[string]
		v low.ValueReference[*PathItem]
	}
	type nodeItem struct {
		currentNode *yaml.Node
		pathNode    *yaml.Node
	}
	pathsMap := make(map[low.KeyReference[string]]low.ValueReference[*PathItem])
	in := make(chan nodeItem)
	out := make(chan pathBuildResult)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2) // input and output goroutines.

	// TranslatePipeline input.
	go func() {
		defer func() {
			close(in)
			wg.Done()
		}()
		skip := false
		var currentNode *yaml.Node
		for i, pathNode := range root.Content {
			if strings.HasPrefix(strings.ToLower(pathNode.Value), "x-") {
				skip = true
				continue
			}
			if skip {
				skip = false
				continue
			}
			if i%2 == 0 {
				currentNode = pathNode
				continue
			}

			select {
			case in <- nodeItem{
				currentNode: currentNode,
				pathNode:    pathNode,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// TranslatePipeline output.
	go func() {
		defer func() {
			cancel()
			wg.Done()
		}()
		for {
			select {
			case result, ok := <-out:
				if !ok {
					return
				}
				pathsMap[result.k] = result.v
			case <-ctx.Done():
				return
			}
		}
	}()

	err := datamodel.TranslatePipeline[nodeItem, pathBuildResult](in, out,
		func(value nodeItem) (pathBuildResult, error) {
			pNode := value.pathNode
			cNode := value.currentNode

			if ok, _, _ := utils.IsNodeRefValue(pNode); ok {
				r, err := low.LocateRefNode(pNode, idx)
				if r != nil {
					pNode = r
					if r.Tag == "" {
						// If it's a node from file, tag is empty
						// If it's a reference we need to extract actual operation node
						pNode = r.Content[0]
					}

					if err != nil {
						if !idx.AllowCircularReferenceResolving() {
							return pathBuildResult{}, fmt.Errorf("path item build failed: %s", err.Error())
						}
					}
				} else {
					return pathBuildResult{}, fmt.Errorf("path item build failed: cannot find reference: %s at line %d, col %d",
						pNode.Content[1].Value, pNode.Content[1].Line, pNode.Content[1].Column)
				}
			}

			path := new(PathItem)
			_ = low.BuildModel(pNode, path)
			err := path.Build(pNode, idx)
			if err != nil {
				return pathBuildResult{}, err
			}

			return pathBuildResult{
				k: low.KeyReference[string]{
					Value:   cNode.Value,
					KeyNode: cNode,
				},
				v: low.ValueReference[*PathItem]{
					Value:     path,
					ValueNode: pNode,
				},
			}, nil
		},
	)
	wg.Wait()
	if err != nil {
		return err
	}

	p.PathItems = pathsMap
	return nil
}

// Hash will return a consistent SHA256 Hash of the PathItem object
func (p *Paths) Hash() [32]byte {
	var f []string
	l := make([]string, len(p.PathItems))
	keys := make(map[string]low.ValueReference[*PathItem])
	z := 0
	for k := range p.PathItems {
		keys[k.Value] = p.PathItems[k]
		l[z] = k.Value
		z++
	}
	sort.Strings(l)
	for k := range l {
		f = append(f, fmt.Sprintf("%s-%s", l[k], low.GenerateHashString(keys[l[k]].Value)))
	}
	ekeys := make([]string, len(p.Extensions))
	z = 0
	for k := range p.Extensions {
		ekeys[z] = fmt.Sprintf("%s-%x", k.Value, sha256.Sum256([]byte(fmt.Sprint(p.Extensions[k].Value))))
		z++
	}
	sort.Strings(ekeys)
	f = append(f, ekeys...)
	return sha256.Sum256([]byte(strings.Join(f, "|")))
}
