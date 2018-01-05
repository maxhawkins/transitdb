package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/maxhawkins/transitdb"
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func New() *Client {
	return &Client{
		HTTPClient: http.DefaultClient,
	}
}

func (c *Client) SendOffers(ctx context.Context, offers []transitdb.Offer) error {
	buf := bytes.NewBuffer(nil)

	for _, offer := range offers {
		if offer.Cost == 0 {
			continue
		}

		if err := json.NewEncoder(buf).Encode(offer); err != nil {
			return err
		}
	}

	req, err := http.NewRequest("POST", c.BaseURL+"/offers", buf)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("send offers: %s", resp.Status)
	}

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("transitdb reply: %s", err)
	}

	return nil
}
