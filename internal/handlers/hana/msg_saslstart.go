package hana

import (
	"context"
	"fmt"

	"github.com/FerretDB/FerretDB/internal/handlers/commonerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/FerretDB/FerretDB/internal/wire"
)

// MsgCollMod implements HandlerInterface.
func (h *Handler) MsgSASLStart(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	return nil, commonerrors.NewCommandErrorMsg(commonerrors.ErrNotImplemented, fmt.Sprintf("%s command is not implemented yet", must.NotFail(msg.Document()).Command()))
}
