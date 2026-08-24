package background

import messagepb "github.com/adrien19/chronoqueue/api/message/v1"

func cloneHeaders(headers []*messagepb.Message_Metadata_Header) []*messagepb.Message_Metadata_Header {
	if len(headers) == 0 {
		return nil
	}

	cloned := make([]*messagepb.Message_Metadata_Header, len(headers))
	for i, header := range headers {
		if header == nil {
			continue
		}
		cloned[i] = &messagepb.Message_Metadata_Header{
			Key:   header.GetKey(),
			Value: append([]byte(nil), header.GetValue()...),
		}
	}
	return cloned
}
