package main

import (
	"crypto/tls"
	"errors"
	ftpserver "github.com/fclairamb/ftpserverlib"
	log "github.com/fclairamb/go-log"
	gkwrap "github.com/fclairamb/go-log/gokit"
	"github.com/spf13/afero"
	"sync"
)

type Driver struct {
	logger          log.Logger
	nbClients       uint32
	nbClientsSync   sync.Mutex
	zeroClientEvent chan error
	tlsOnce         sync.Once
	tlsConfig       *tls.Config
	tlsError        error
}

func (d *Driver) GetSettings() (*ftpserver.Settings, error) {
	return &ftpserver.Settings{
		PassiveTransferPortRange: &ftpserver.PortRange{
			Start: 2122,
			End:   2130,
		},
	}, nil
}

func (d *Driver) ClientConnected(cc ftpserver.ClientContext) (string, error) {
	d.nbClientsSync.Lock()
	defer d.nbClientsSync.Unlock()
	d.nbClients++
	d.logger.Info(
		"Client connected",
		"clientId", cc.ID(),
		"remoteAddr", cc.RemoteAddr(),
		"nbClients", d.nbClients,
	)
	return "ftpserver", nil
}

func (d *Driver) ClientDisconnected(cc ftpserver.ClientContext) {
	d.nbClientsSync.Lock()
	defer d.nbClientsSync.Unlock()

	d.nbClients--

	d.logger.Info(
		"Client disconnected",
		"clientId", cc.ID(),
		"remoteAddr", cc.RemoteAddr(),
		"nbClients", d.nbClients,
	)
	d.considerEnd()
}

func (d *Driver) considerEnd() {
	if d.nbClients == 0 && d.zeroClientEvent != nil {
		d.zeroClientEvent <- nil
		close(d.zeroClientEvent)
	}
}

// The ClientDriver is the internal structure used for handling the client. At this stage it's limited to the afero.Fs
type ClientDriver struct {
	afero.Fs
}

func (d *Driver) AuthUser(cc ftpserver.ClientContext, user, pass string) (ftpserver.ClientDriver, error) {
	accFs := afero.NewBasePathFs(afero.NewOsFs(), "/Users/shawn/code/lifs_go")
	return accFs, nil
}

func (d *Driver) GetTLSConfig() (*tls.Config, error) {
	return nil, errors.New("not enabled")
}

// NewServer creates a server instance
func NewServer(logger log.Logger) (ftpserver.MainDriver, error) {
	return &Driver{
		logger: logger,
	}, nil
}

func main() {
	logger := gkwrap.New()

	driver, err := NewServer(logger)
	if err != nil {
		logger.Error("Problem creating server", "err", err)
	}
	ftpServer := ftpserver.NewFtpServer(driver)

	if err := ftpServer.ListenAndServe(); err != nil {
		logger.Error("Problem listening", "err", err)
	}
}
