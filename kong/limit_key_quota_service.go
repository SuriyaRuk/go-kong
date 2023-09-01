package kong

import (
	"context"
	"encoding/json"
)

// AbstractLimitKeyQuotaService handles limit-key-quota credentials in Kong.
type AbstractLimitKeyQuotaService interface {
	// Create creates a limit-key-quota credential in Kong
	Create(ctx context.Context, consumerUsernameOrID *string, limitKeyQuota *LimitKeyQuota) (*LimitKeyQuota, error)
	// Get fetches a limit-key-quota credential from Kong.
	Get(ctx context.Context, consumerUsernameOrID, keyOrID *string) (*LimitKeyQuota, error)
	// Update updates a limit-key-quota credential in Kong
	Update(ctx context.Context, consumerUsernameOrID *string, limitKeyQuota *LimitKeyQuota) (*LimitKeyQuota, error)
	// Delete deletes a limit-key-quota credential in Kong
	Delete(ctx context.Context, consumerUsernameOrID, keyOrID *string) error
	// List fetches a list of limit-key-quota credentials in Kong.
	List(ctx context.Context, opt *ListOpt) ([]*LimitKeyQuota, *ListOpt, error)
	// ListAll fetches all limit-key-quota credentials in Kong.
	ListAll(ctx context.Context) ([]*LimitKeyQuota, error)
	// ListForConsumer fetches a list of limit-key-quota credentials
	ListForConsumer(ctx context.Context, consumerUsernameOrID *string, opt *ListOpt) ([]*LimitKeyQuota, *ListOpt, error)
}

// LimitKeyQuotaService handles limit-key-quota credentials in Kong.
type LimitKeyQuotaService service

// Create creates a limit-key-quota credential in Kong
// If an ID is specified, it will be used to
// create a limit-key-quota in Kong, otherwise an ID
// is auto-generated.
func (s *LimitKeyQuotaService) Create(ctx context.Context,
	consumerUsernameOrID *string, limitKeyQuota *LimitKeyQuota,
) (*LimitKeyQuota, error) {
	cred, err := s.client.credentials.Create(ctx, "limit-key-quota",
		consumerUsernameOrID, limitKeyQuota)
	if err != nil {
		return nil, err
	}

	var createdLimitKeyQuota LimitKeyQuota
	err = json.Unmarshal(cred, &createdLimitKeyQuota)
	if err != nil {
		return nil, err
	}

	return &createdLimitKeyQuota, nil
}

// Get fetches a limit-key-quota credential from Kong.
func (s *LimitKeyQuotaService) Get(ctx context.Context,
	consumerUsernameOrID, keyOrID *string,
) (*LimitKeyQuota, error) {
	cred, err := s.client.credentials.Get(ctx, "limit-key-quota",
		consumerUsernameOrID, keyOrID)
	if err != nil {
		return nil, err
	}

	var limitKeyQuota LimitKeyQuota
	err = json.Unmarshal(cred, &limitKeyQuota)
	if err != nil {
		return nil, err
	}

	return &limitKeyQuota, nil
}

// Update updates a limit-key-quota credential in Kong
func (s *LimitKeyQuotaService) Update(ctx context.Context,
	consumerUsernameOrID *string, limitKeyQuota *LimitKeyQuota,
) (*LimitKeyQuota, error) {
	cred, err := s.client.credentials.Update(ctx, "limit-key-quota",
		consumerUsernameOrID, limitKeyQuota)
	if err != nil {
		return nil, err
	}

	var updatedLimitKeyQuota LimitKeyQuota
	err = json.Unmarshal(cred, &updatedLimitKeyQuota)
	if err != nil {
		return nil, err
	}

	return &updatedLimitKeyQuota, nil
}

// Delete deletes a limit-key-quota credential in Kong
func (s *LimitKeyQuotaService) Delete(ctx context.Context,
	consumerUsernameOrID, keyOrID *string,
) error {
	return s.client.credentials.Delete(ctx, "limit-key-quota",
		consumerUsernameOrID, keyOrID)
}

// List fetches a list of limit-key-quota credentials in Kong.
// opt can be used to control pagination.
func (s *LimitKeyQuotaService) List(ctx context.Context,
	opt *ListOpt,
) ([]*LimitKeyQuota, *ListOpt, error) {
	data, next, err := s.client.list(ctx, "/limit-key-quotas", opt)
	if err != nil {
		return nil, nil, err
	}
	var limitKeyQuotas []*LimitKeyQuota
	for _, object := range data {
		b, err := object.MarshalJSON()
		if err != nil {
			return nil, nil, err
		}
		var limitKeyQuota LimitKeyQuota
		err = json.Unmarshal(b, &limitKeyQuota)
		if err != nil {
			return nil, nil, err
		}
		limitKeyQuotas = append(limitKeyQuotas, &limitKeyQuota)
	}

	return limitKeyQuotas, next, nil
}

// ListAll fetches all limit-key-quota credentials in Kong.
// This method can take a while if there
// a lot of limit-key-quota credentials present.
func (s *LimitKeyQuotaService) ListAll(ctx context.Context) ([]*LimitKeyQuota, error) {
	var limitKeyQuotas, data []*LimitKeyQuota
	var err error
	opt := &ListOpt{Size: pageSize}

	for opt != nil {
		data, opt, err = s.List(ctx, opt)
		if err != nil {
			return nil, err
		}
		limitKeyQuotas = append(limitKeyQuotas, data...)
	}
	return limitKeyQuotas, nil
}

// ListForConsumer fetches a list of limit-key-quota credentials
// in Kong associated with a specific consumer.
// opt can be used to control pagination.
func (s *LimitKeyQuotaService) ListForConsumer(ctx context.Context,
	consumerUsernameOrID *string, opt *ListOpt,
) ([]*LimitKeyQuota, *ListOpt, error) {
	data, next, err := s.client.list(ctx,
		"/consumers/"+*consumerUsernameOrID+"/limit-key-quota", opt)
	if err != nil {
		return nil, nil, err
	}
	var limitKeyQuotas []*LimitKeyQuota
	for _, object := range data {
		b, err := object.MarshalJSON()
		if err != nil {
			return nil, nil, err
		}
		var limitKeyQuota LimitKeyQuota
		err = json.Unmarshal(b, &limitKeyQuota)
		if err != nil {
			return nil, nil, err
		}
		limitKeyQuotas = append(limitKeyQuotas, &limitKeyQuota)
	}

	return limitKeyQuotas, next, nil
}
