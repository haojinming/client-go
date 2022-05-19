package locate

import (
	"context"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/util/codec"

	pd "github.com/tikv/pd/client"
)

type CodecPDClientV2 struct {
	*CodecPDClient
	mode client.Mode
}

func NewCodecPDClientV2(client pd.Client, mode client.Mode) *CodecPDClientV2 {
	codecClient := NewCodeCPDClient(client)
	return &CodecPDClientV2{codecClient, mode}
}

// GetRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClientV2) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	key = client.EncodeV2Key(c.mode, key)
	region, err := c.CodecPDClient.GetRegion(ctx, key, opts...)
	return c.processRegionResult(region, err)
}

// GetPrevRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClientV2) GetPrevRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	key = client.EncodeV2Key(c.mode, key)
	region, err := c.CodecPDClient.GetPrevRegion(ctx, key, opts...)
	return c.processRegionResult(region, err)
}

// GetRegionByID encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClientV2) GetRegionByID(ctx context.Context, regionID uint64, opts ...pd.GetRegionOption) (*pd.Region, error) {
	region, err := c.CodecPDClient.GetRegionByID(ctx, regionID, opts...)
	return c.processRegionResult(region, err)
}

// ScanRegions encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClientV2) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*pd.Region, error) {
	start, end := client.EncodeV2Range(c.mode, startKey, endKey)
	regions, err := c.CodecPDClient.ScanRegions(ctx, start, end, limit)
	if err != nil {
		return nil, err
	}
	for i := range regions {
		region, _ := c.processRegionResult(regions[i], nil)
		regions[i] = region
	}
	return regions, nil
}

func (c *CodecPDClientV2) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	var keys [][]byte
	for i := range splitKeys {
		withPrefix := client.EncodeV2Key(c.mode, splitKeys[i])
		keys = append(keys, codec.EncodeBytes(nil, withPrefix))
	}
	return c.CodecPDClient.SplitRegions(ctx, keys, opts...)
}

func (c *CodecPDClientV2) processRegionResult(region *pd.Region, err error) (*pd.Region, error) {
	if err != nil {
		return nil, err
	}

	if region != nil {
		// TODO(@iosmanthus): enable buckets support.
		region.Buckets = nil

		region.Meta.StartKey, region.Meta.EndKey =
			client.MapV2RangeToV1(c.mode, region.Meta.StartKey, region.Meta.EndKey)
	}

	return region, nil
}
