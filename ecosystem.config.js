const pipelineSchedules = [
    { name: "api_beneficiarios_iraca", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
    { name: "api_edu_escolar", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
    { name: "api_edu_superior", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
    { name: "api_erradicacion_cultivos_coca", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
    { name: "api_familias_accion", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
    { name: "api_indice_riesgo_irca", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
    { name: "api_minerales", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
    { name: "api_produc_gas", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
    { name: "api_produc_petroleo", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
    { name: "api_regalias", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
    { name: "api_victimas", trigger: "interval", seconds: 60, startDelaySeconds: 60 },
];

module.exports = {
    apps: [
        {
            name: "etl_scheduler",
            script: "uv",
            args: ["run", "-m", "src.scheduler"],
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "none",
            watch: false,
            autorestart: true,
            restart_delay: 5000,
            max_memory_restart: "200M",
            env: {
                PYTHONUNBUFFERED: "1",
                ETL_MAX_CONCURRENT_JOBS: "1",
                ETL_JOB_MISFIRE_GRACE_TIME: "30",
                ETL_SCHEDULES: JSON.stringify(pipelineSchedules),
            },
        },
    ],
};
