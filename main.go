package main

import (
	"bytes"
	"flag"
	"log"
	"net/http"
	"os"

	"code.crute.us/mcrute/ses-smtpd-proxy/smtpd"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ses"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	SesSizeLimit = 10000000
	DefaultAddr  = ":2500"
)

var sesClient *ses.SES

var (
	emailSent = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "smtpd",
		Name:      "email_send_success_total",
		Help:      "Total number of successfuly sent emails",
	})
	emailError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "smtpd",
		Name:      "email_send_fail_total",
		Help:      "Total number emails that failed to send",
	}, []string{"type"})
	sesError = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "smtpd",
		Name:      "ses_error_total",
		Help:      "Total number errors with SES",
	})
	credentialRenewalSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "smtpd",
		Name:      "credential_renewal_success_total",
		Help:      "Total number successful credential renewals",
	})
	credentialRenewalError = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "smtpd",
		Name:      "credential_renewal_error_total",
		Help:      "Total number errors during credential renewal",
	})
)

type Envelope struct {
	from  string
	rcpts []*string
	b     bytes.Buffer
}

func (e *Envelope) AddRecipient(rcpt smtpd.MailAddress) error {
	email := rcpt.Email()
	e.rcpts = append(e.rcpts, &email)
	return nil
}

func (e *Envelope) BeginData() error {
	if len(e.rcpts) == 0 {
		emailError.With(prometheus.Labels{"type": "no valid recipients"}).Inc()
		return smtpd.SMTPError("554 5.5.1 Error: no valid recipients")
	}
	return nil
}

func (e *Envelope) Write(line []byte) error {
	e.b.Write(line)
	if e.b.Len() > SesSizeLimit { // SES limitation
		emailError.With(prometheus.Labels{"type": "minimum message size exceed"}).Inc()
		log.Printf("message size %d exceeds SES limit of %d", e.b.Len(), SesSizeLimit)
		return smtpd.SMTPError("554 5.5.1 Error: maximum message size exceeded")
	}
	return nil
}

func (e *Envelope) logMessageSend() {
	dr := make([]string, len(e.rcpts))
	for i := range e.rcpts {
		dr[i] = *e.rcpts[i]
	}
	log.Printf("sending message from %+v to %+v", e.from, dr)
	emailSent.Inc()
}

func (e *Envelope) Close() error {
	r := &ses.SendRawEmailInput{
		Source:       &e.from,
		Destinations: e.rcpts,
		RawMessage:   &ses.RawMessage{Data: e.b.Bytes()},
	}
	_, err := sesClient.SendRawEmail(r)
	if err != nil {
		log.Printf("ERROR: ses: %v", err)
		emailError.With(prometheus.Labels{"type": "ses error"}).Inc()
		sesError.Inc()
		return smtpd.SMTPError("451 4.5.1 Temporary server error. Please try again later")
	}
	e.logMessageSend()
	return err
}

func makeSesClient() (*ses.SES, error) {
	var err error
	var s *session.Session

	s, err = session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")})
	if err != nil {
		return nil, err
	}

	return ses.New(s), nil
}

func main() {
	var err error

	disablePrometheus := flag.Bool("disable-prometheus", false, "Disables prometheus metrics server")
	prometheusBind := flag.String("prometheus-bind", ":2501", "Address/port on which to bind Prometheus server")

	flag.Parse()

	sesClient, err = makeSesClient()
	if err != nil {
		log.Fatalf("Error creating AWS session: %s", err)
	}

	addr := DefaultAddr
	if flag.Arg(0) != "" {
		addr = flag.Arg(0)
	} else if flag.NArg() > 1 {
		log.Fatalf("usage: %s [listen_host:port]", os.Args[0])
	}

	if !*disablePrometheus {
		sm := http.NewServeMux()
		ps := &http.Server{Addr: *prometheusBind, Handler: sm}
		sm.Handle("/metrics", promhttp.Handler())
		go ps.ListenAndServe()
	}

	s := &smtpd.Server{
		Addr: addr,
		OnNewMail: func(c smtpd.Connection, from smtpd.MailAddress) (smtpd.Envelope, error) {
			return &Envelope{from: from.Email()}, nil
		},
	}

	log.Printf("ListenAndServe on %s", addr)
	if err := s.ListenAndServe(); err != nil {
		log.Fatalf("ListenAndServe: %v", err)
	}
}
