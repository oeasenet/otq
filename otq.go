package otq

import (
	"github.com/hibiken/asynq"
	"github.com/oeasenet/jog"
	"os"
	"runtime"
	"time"
)

type Otq struct {
	asynqServer      *asynq.Server
	asynqServerMux   *asynq.ServeMux
	scheduler        *asynq.Scheduler
	scheduledTaskIds []string
	client           *asynq.Client
}

var instance *Otq

func NewOtq(redisAddress string, redisUsername string, redisPassword string, db int) *Otq {
	if instance != nil {
		jog.Warn("OTQ is already initialized")
		return instance
	}
	if redisAddress == "" {
		jog.Panic("redis address is empty")
	}

	redConnOpts := asynq.RedisClientOpt{
		Network:      "tcp",
		Addr:         redisAddress,
		Username:     redisUsername,
		Password:     redisPassword,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10 * runtime.NumCPU(),
	}

	aServer := asynq.NewServer(redConnOpts, asynq.Config{
		// one task at a time to make sure the queue is processed in order, smaller the number faster the queue is processed but order is not guaranteed
		Concurrency: 10,
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
		Logger:                   jog.GetSugar(),
		ShutdownTimeout:          10 * time.Second,
		HealthCheckInterval:      10 * time.Second,
		DelayedTaskCheckInterval: 5 * time.Second,
		GroupGracePeriod:         30 * time.Second,
	})
	timezone := os.Getenv("TZ")
	var loc *time.Location
	var err error
	if timezone != "" {
		loc, err = time.LoadLocation(timezone)
		if err != nil {
			jog.Panic("invalid timezone")
		}
	} else {
		loc = time.UTC
	}
	aScheduler := asynq.NewScheduler(redConnOpts, &asynq.SchedulerOpts{
		Logger:   jog.GetSugar(),
		Location: loc,
	})

	aClient := asynq.NewClient(redConnOpts)

	instance = &Otq{
		asynqServer:    aServer,
		asynqServerMux: asynq.NewServeMux(),
		scheduler:      aScheduler,
		client:         aClient,
	}

	return instance
}

func (o *Otq) AddHandler(taskTypeName string, handler asynq.HandlerFunc) *Otq {
	o.asynqServerMux.HandleFunc(taskTypeName, handler)
	return o
}

func (o *Otq) AddScheduledTask(cronExpression string, task *asynq.Task) *Otq {
	//@every <duration> or cron expression
	eId, err := o.scheduler.Register(cronExpression, task)
	if err != nil {
		jog.Error(err)
		return o
	}
	o.scheduledTaskIds = append(o.scheduledTaskIds, eId)
	return o
}

func (o *Otq) Enqueue(task *asynq.Task, opts ...asynq.Option) *Otq {
	_, err := o.client.Enqueue(task, opts...)
	if err != nil {
		jog.Error(err)
		return o
	}
	return o
}

func (o *Otq) Start() {
	err := o.asynqServer.Start(o.asynqServerMux)
	if err != nil {
		jog.Panic(err)
	}
	if len(o.scheduledTaskIds) > 0 {
		err = o.scheduler.Start()
		if err != nil {
			jog.Panic(err)
		}
	}
}

func (o *Otq) Close() {
	_ = o.client.Close()
	o.asynqServer.Shutdown()
	o.scheduler.Shutdown()
}

func AddHandler(taskTypeName string, handler asynq.HandlerFunc) {
	instance.AddHandler(taskTypeName, handler)
}

func AddScheduledTask(cronExpression string, task *asynq.Task) {
	instance.AddScheduledTask(cronExpression, task)
}

func Enqueue(task *asynq.Task, opts ...asynq.Option) {
	instance.Enqueue(task, opts...)
}

func Start() {
	instance.Start()
}

func Close() {
	instance.Close()
}
