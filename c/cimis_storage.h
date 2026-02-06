#ifndef CIMIS_STORAGE_H
#define CIMIS_STORAGE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * CIMIS Time-Series Storage Engine
 * High-performance C implementation for binary record encoding/decoding
 */

/* Constants */
#define CIMIS_EPOCH_YEAR 1985
#define CIMIS_DAILY_RECORD_SIZE 16
#define CIMIS_HOURLY_RECORD_SIZE 24

/* Fixed-point scaling factors */
#define TEMP_SCALE 10.0f
#define ET_DAILY_SCALE 100.0f
#define ET_HOURLY_SCALE 1000.0f
#define WIND_SCALE 10.0f
#define SOLAR_SCALE 10.0f
#define PRECIP_SCALE 100.0f
#define VAPOR_SCALE 100.0f
#define WIND_DIR_SCALE 0.5f

/* Daily record structure - packed 16 bytes
 * Binary layout (little-endian):
 * Offset  Type    Field
 * 0       u32     Days since epoch (1985-01-01)
 * 4       u16     Station ID
 * 6       i16     Temperature (×10, hundredths of °C)
 * 8       i16     ET (×100, hundredths of mm)
 * 10      u16     Wind speed (×10, tenths of m/s)
 * 12      u8      Relative humidity (%)
 * 13      u8      Solar radiation (×10, tenths of MJ/m²)
 * 14      u8      QC flags (bit-packed)
 * 15      u8      Reserved/padding
 */
typedef struct __attribute__((packed)) {
    uint32_t timestamp;       /* Days since epoch */
    uint16_t station_id;      /* Station identifier */
    int16_t  temperature;     /* Scaled: value / 10 = °C */
    int16_t  et;             /* Scaled: value / 100 = mm */
    uint16_t wind_speed;      /* Scaled: value / 10 = m/s */
    uint8_t  humidity;        /* Percentage (0-100) */
    uint8_t  solar_radiation; /* Scaled: value / 10 = MJ/m² */
    uint8_t  qc_flags;        /* Bit-packed quality flags */
    uint8_t  reserved;        /* Padding/reserved */
} cimis_daily_record_t;

/* Hourly record structure - packed 24 bytes
 * Binary layout (little-endian):
 * Offset  Type    Field
 * 0       u32     Hours since epoch (1985-01-01 00:00)
 * 4       u16     Station ID
 * 6       i16     Temperature (×10, hundredths of °C)
 * 8       i16     ET (×1000, thousandths of mm)
 * 10      u16     Wind speed (×10, tenths of m/s)
 * 12      u8      Wind direction (0-360 degrees, scaled /2)
 * 13      u8      Relative humidity (%)
 * 14      u16     Solar radiation (W/m²)
 * 16      u16     Precipitation (×100, hundredths of mm)
 * 18      u16     Vapor pressure (×100, hundredths of kPa)
 * 20      u8      QC flags (bit-packed)
 * 21      u8      Reserved
 * 22-23   --      Padding
 */
typedef struct __attribute__((packed)) {
    uint32_t timestamp;       /* Hours since epoch */
    uint16_t station_id;      /* Station identifier */
    int16_t  temperature;     /* Scaled: value / 10 = °C */
    int16_t  et;             /* Scaled: value / 1000 = mm */
    uint16_t wind_speed;      /* Scaled: value / 10 = m/s */
    uint8_t  wind_direction;  /* Scaled: value * 2 = degrees */
    uint8_t  humidity;        /* Percentage (0-100) */
    uint16_t solar_radiation; /* W/m² */
    uint16_t precipitation;   /* Scaled: value / 100 = mm */
    uint16_t vapor_pressure;  /* Scaled: value / 100 = kPa */
    uint8_t  qc_flags;        /* Bit-packed quality flags */
    uint8_t  reserved;        /* Reserved */
    uint8_t  pad[2];         /* Padding to 24 bytes */
} cimis_hourly_record_t;

/* QC Flag bits */
#define QC_TEMPERATURE   0x01
#define QC_ET            0x02
#define QC_WIND_SPEED    0x04
#define QC_HUMIDITY      0x08
#define QC_SOLAR_RAD     0x10
#define QC_PRECIPITATION 0x20
#define QC_COMPUTED      0x40
#define QC_ESTIMATED     0x80

/* Record batch for vectorized operations */
typedef struct {
    uint16_t station_id;
    uint32_t count;
    union {
        cimis_daily_record_t  *daily;
        cimis_hourly_record_t *hourly;
    } records;
} cimis_record_batch_t;

/* Result codes */
typedef enum {
    CIMIS_OK = 0,
    CIMIS_ERR_NULL_PTR = -1,
    CIMIS_ERR_INVALID_SIZE = -2,
    CIMIS_ERR_BUFFER_TOO_SMALL = -3,
    CIMIS_ERR_OUT_OF_MEMORY = -4,
    CIMIS_ERR_INVALID_TIMESTAMP = -5
} cimis_result_t;

/* Function Prototypes */

/* Fixed-point conversion functions */
static inline int16_t cimis_float_to_fixed_temp(float val) {
    return (int16_t)(val * TEMP_SCALE);
}

static inline float cimis_fixed_to_float_temp(int16_t val) {
    return (float)val / TEMP_SCALE;
}

static inline int16_t cimis_float_to_fixed_et_daily(float val) {
    return (int16_t)(val * ET_DAILY_SCALE);
}

static inline float cimis_fixed_to_float_et_daily(int16_t val) {
    return (float)val / ET_DAILY_SCALE;
}

static inline int16_t cimis_float_to_fixed_et_hourly(float val) {
    return (int16_t)(val * ET_HOURLY_SCALE);
}

static inline float cimis_fixed_to_float_et_hourly(int16_t val) {
    return (float)val / ET_HOURLY_SCALE;
}

static inline uint16_t cimis_float_to_fixed_wind(float val) {
    return (uint16_t)(val * WIND_SCALE);
}

static inline float cimis_fixed_to_float_wind(uint16_t val) {
    return (float)val / WIND_SCALE;
}

static inline uint8_t cimis_float_to_fixed_solar(float val) {
    return (uint8_t)(val * SOLAR_SCALE);
}

static inline float cimis_fixed_to_float_solar(uint8_t val) {
    return (float)val / SOLAR_SCALE;
}

static inline uint8_t cimis_float_to_fixed_wind_dir(float val) {
    return (uint8_t)(val * WIND_DIR_SCALE);
}

static inline float cimis_fixed_to_float_wind_dir(uint8_t val) {
    return (float)val / WIND_DIR_SCALE;
}

static inline uint16_t cimis_float_to_fixed_precip(float val) {
    return (uint16_t)(val * PRECIP_SCALE);
}

static inline float cimis_fixed_to_float_precip(uint16_t val) {
    return (float)val / PRECIP_SCALE;
}

static inline uint16_t cimis_float_to_fixed_vapor(float val) {
    return (uint16_t)(val * VAPOR_SCALE);
}

static inline float cimis_fixed_to_float_vapor(uint16_t val) {
    return (float)val / VAPOR_SCALE;
}

/* Timestamp conversion */
uint32_t cimis_date_to_days_since_epoch(int year, int month, int day);
void cimis_days_since_epoch_to_date(uint32_t days, int *year, int *month, int *day);
uint32_t cimis_datetime_to_hours_since_epoch(int year, int month, int day, int hour);

/* Record encoding/decoding */
cimis_result_t cimis_encode_daily_record(const cimis_daily_record_t *record, uint8_t *buffer, size_t buffer_size);
cimis_result_t cimis_decode_daily_record(const uint8_t *buffer, size_t buffer_size, cimis_daily_record_t *record);

cimis_result_t cimis_encode_hourly_record(const cimis_hourly_record_t *record, uint8_t *buffer, size_t buffer_size);
cimis_result_t cimis_decode_hourly_record(const uint8_t *buffer, size_t buffer_size, cimis_hourly_record_t *record);

/* Batch encoding/decoding */
size_t cimis_encode_daily_batch(const cimis_daily_record_t *records, uint32_t count, uint8_t *buffer, size_t buffer_size);
size_t cimis_decode_daily_batch(const uint8_t *buffer, size_t buffer_size, cimis_daily_record_t *records, uint32_t max_count);

size_t cimis_encode_hourly_batch(const cimis_hourly_record_t *records, uint32_t count, uint8_t *buffer, size_t buffer_size);
size_t cimis_decode_hourly_batch(const uint8_t *buffer, size_t buffer_size, cimis_hourly_record_t *records, uint32_t max_count);

/* Validation */
bool cimis_validate_daily_record(const cimis_daily_record_t *record);
bool cimis_validate_hourly_record(const cimis_hourly_record_t *record);

/* Memory-efficient sequential read (for mobile/embedded) */
typedef struct {
    const uint8_t *buffer;
    size_t buffer_size;
    size_t current_offset;
    uint32_t record_count;
    bool is_hourly;
} cimis_record_iterator_t;

cimis_result_t cimis_iterator_init(cimis_record_iterator_t *iter, const uint8_t *buffer, size_t buffer_size, bool is_hourly);
bool cimis_iterator_has_next(const cimis_record_iterator_t *iter);
cimis_result_t cimis_iterator_next_daily(cimis_record_iterator_t *iter, cimis_daily_record_t *record);
cimis_result_t cimis_iterator_next_hourly(cimis_record_iterator_t *iter, cimis_hourly_record_t *record);

/* Statistics calculation */
typedef struct {
    float min_temp;
    float max_temp;
    float avg_temp;
    float total_et;
    uint32_t record_count;
} cimis_daily_stats_t;

void cimis_calculate_daily_stats(const cimis_daily_record_t *records, uint32_t count, cimis_daily_stats_t *stats);

#ifdef __cplusplus
}
#endif

#endif /* CIMIS_STORAGE_H */
