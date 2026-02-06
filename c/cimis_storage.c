#include "cimis_storage.h"
#include <string.h>
#include <stdlib.h>
#include <math.h>

/* Days in each month (non-leap year) */
static const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

/* Check if year is a leap year */
static int is_leap_year(int year) {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

/* Get days in month accounting for leap years */
static int get_days_in_month(int year, int month) {
    if (month == 2 && is_leap_year(year)) {
        return 29;
    }
    return days_in_month[month - 1];
}

/* Calculate days since epoch (January 1, 1985) */
uint32_t cimis_date_to_days_since_epoch(int year, int month, int day) {
    uint32_t days = 0;
    int y;
    
    /* Add days for complete years */
    for (y = CIMIS_EPOCH_YEAR; y < year; y++) {
        days += is_leap_year(y) ? 366 : 365;
    }
    
    /* Add days for complete months in current year */
    for (int m = 1; m < month; m++) {
        days += get_days_in_month(year, m);
    }
    
    /* Add days in current month */
    days += day - 1;
    
    return days;
}

/* Convert days since epoch back to date */
void cimis_days_since_epoch_to_date(uint32_t days, int *year, int *month, int *day) {
    int y = CIMIS_EPOCH_YEAR;
    int remaining_days = days;
    
    /* Find the year */
    while (1) {
        int year_days = is_leap_year(y) ? 366 : 365;
        if (remaining_days < year_days) {
            break;
        }
        remaining_days -= year_days;
        y++;
    }
    
    *year = y;
    
    /* Find the month */
    int m = 1;
    while (1) {
        int month_days = get_days_in_month(y, m);
        if (remaining_days < month_days) {
            break;
        }
        remaining_days -= month_days;
        m++;
    }
    
    *month = m;
    *day = remaining_days + 1;
}

/* Convert datetime to hours since epoch */
uint32_t cimis_datetime_to_hours_since_epoch(int year, int month, int day, int hour) {
    uint32_t days = cimis_date_to_days_since_epoch(year, month, day);
    return days * 24 + hour;
}

/* Encode a single daily record */
cimis_result_t cimis_encode_daily_record(const cimis_daily_record_t *record, uint8_t *buffer, size_t buffer_size) {
    if (record == NULL || buffer == NULL) {
        return CIMIS_ERR_NULL_PTR;
    }
    
    if (buffer_size < CIMIS_DAILY_RECORD_SIZE) {
        return CIMIS_ERR_BUFFER_TOO_SMALL;
    }
    
    /* Write in little-endian order */
    buffer[0] = (uint8_t)(record->timestamp & 0xFF);
    buffer[1] = (uint8_t)((record->timestamp >> 8) & 0xFF);
    buffer[2] = (uint8_t)((record->timestamp >> 16) & 0xFF);
    buffer[3] = (uint8_t)((record->timestamp >> 24) & 0xFF);
    
    buffer[4] = (uint8_t)(record->station_id & 0xFF);
    buffer[5] = (uint8_t)((record->station_id >> 8) & 0xFF);
    
    buffer[6] = (uint8_t)(record->temperature & 0xFF);
    buffer[7] = (uint8_t)((record->temperature >> 8) & 0xFF);
    
    buffer[8] = (uint8_t)(record->et & 0xFF);
    buffer[9] = (uint8_t)((record->et >> 8) & 0xFF);
    
    buffer[10] = (uint8_t)(record->wind_speed & 0xFF);
    buffer[11] = (uint8_t)((record->wind_speed >> 8) & 0xFF);
    
    buffer[12] = record->humidity;
    buffer[13] = record->solar_radiation;
    buffer[14] = record->qc_flags;
    buffer[15] = record->reserved;
    
    return CIMIS_OK;
}

/* Decode a single daily record */
cimis_result_t cimis_decode_daily_record(const uint8_t *buffer, size_t buffer_size, cimis_daily_record_t *record) {
    if (buffer == NULL || record == NULL) {
        return CIMIS_ERR_NULL_PTR;
    }
    
    if (buffer_size < CIMIS_DAILY_RECORD_SIZE) {
        return CIMIS_ERR_BUFFER_TOO_SMALL;
    }
    
    /* Read in little-endian order */
    record->timestamp = 
        ((uint32_t)buffer[0]) |
        ((uint32_t)buffer[1] << 8) |
        ((uint32_t)buffer[2] << 16) |
        ((uint32_t)buffer[3] << 24);
    
    record->station_id = 
        ((uint16_t)buffer[4]) |
        ((uint16_t)buffer[5] << 8);
    
    record->temperature = (int16_t)(
        ((uint16_t)buffer[6]) |
        ((uint16_t)buffer[7] << 8));
    
    record->et = (int16_t)(
        ((uint16_t)buffer[8]) |
        ((uint16_t)buffer[9] << 8));
    
    record->wind_speed = 
        ((uint16_t)buffer[10]) |
        ((uint16_t)buffer[11] << 8);
    
    record->humidity = buffer[12];
    record->solar_radiation = buffer[13];
    record->qc_flags = buffer[14];
    record->reserved = buffer[15];
    
    return CIMIS_OK;
}

/* Encode a batch of daily records */
size_t cimis_encode_daily_batch(const cimis_daily_record_t *records, uint32_t count, 
                                uint8_t *buffer, size_t buffer_size) {
    if (records == NULL || buffer == NULL || count == 0) {
        return 0;
    }
    
    size_t required_size = count * CIMIS_DAILY_RECORD_SIZE;
    if (buffer_size < required_size) {
        return 0;
    }
    
    for (uint32_t i = 0; i < count; i++) {
        cimis_result_t result = cimis_encode_daily_record(
            &records[i], 
            buffer + (i * CIMIS_DAILY_RECORD_SIZE), 
            CIMIS_DAILY_RECORD_SIZE
        );
        if (result != CIMIS_OK) {
            return 0;
        }
    }
    
    return required_size;
}

/* Decode a batch of daily records */
size_t cimis_decode_daily_batch(const uint8_t *buffer, size_t buffer_size,
                                  cimis_daily_record_t *records, uint32_t max_count) {
    if (buffer == NULL || records == NULL || max_count == 0) {
        return 0;
    }
    
    uint32_t record_count = buffer_size / CIMIS_DAILY_RECORD_SIZE;
    if (record_count > max_count) {
        record_count = max_count;
    }
    
    for (uint32_t i = 0; i < record_count; i++) {
        cimis_result_t result = cimis_decode_daily_record(
            buffer + (i * CIMIS_DAILY_RECORD_SIZE),
            CIMIS_DAILY_RECORD_SIZE,
            &records[i]
        );
        if (result != CIMIS_OK) {
            return i;
        }
    }
    
    return record_count;
}

/* Encode a single hourly record */
cimis_result_t cimis_encode_hourly_record(const cimis_hourly_record_t *record, uint8_t *buffer, size_t buffer_size) {
    if (record == NULL || buffer == NULL) {
        return CIMIS_ERR_NULL_PTR;
    }
    
    if (buffer_size < CIMIS_HOURLY_RECORD_SIZE) {
        return CIMIS_ERR_BUFFER_TOO_SMALL;
    }
    
    /* Write in little-endian order */
    buffer[0] = (uint8_t)(record->timestamp & 0xFF);
    buffer[1] = (uint8_t)((record->timestamp >> 8) & 0xFF);
    buffer[2] = (uint8_t)((record->timestamp >> 16) & 0xFF);
    buffer[3] = (uint8_t)((record->timestamp >> 24) & 0xFF);
    
    buffer[4] = (uint8_t)(record->station_id & 0xFF);
    buffer[5] = (uint8_t)((record->station_id >> 8) & 0xFF);
    
    buffer[6] = (uint8_t)(record->temperature & 0xFF);
    buffer[7] = (uint8_t)((record->temperature >> 8) & 0xFF);
    
    buffer[8] = (uint8_t)(record->et & 0xFF);
    buffer[9] = (uint8_t)((record->et >> 8) & 0xFF);
    
    buffer[10] = (uint8_t)(record->wind_speed & 0xFF);
    buffer[11] = (uint8_t)((record->wind_speed >> 8) & 0xFF);
    
    buffer[12] = record->wind_direction;
    buffer[13] = record->humidity;
    
    buffer[14] = (uint8_t)(record->solar_radiation & 0xFF);
    buffer[15] = (uint8_t)((record->solar_radiation >> 8) & 0xFF);
    
    buffer[16] = (uint8_t)(record->precipitation & 0xFF);
    buffer[17] = (uint8_t)((record->precipitation >> 8) & 0xFF);
    
    buffer[18] = (uint8_t)(record->vapor_pressure & 0xFF);
    buffer[19] = (uint8_t)((record->vapor_pressure >> 8) & 0xFF);
    
    buffer[20] = record->qc_flags;
    buffer[21] = record->reserved;
    buffer[22] = 0;
    buffer[23] = 0;
    
    return CIMIS_OK;
}

/* Decode a single hourly record */
cimis_result_t cimis_decode_hourly_record(const uint8_t *buffer, size_t buffer_size, cimis_hourly_record_t *record) {
    if (buffer == NULL || record == NULL) {
        return CIMIS_ERR_NULL_PTR;
    }
    
    if (buffer_size < CIMIS_HOURLY_RECORD_SIZE) {
        return CIMIS_ERR_BUFFER_TOO_SMALL;
    }
    
    /* Read in little-endian order */
    record->timestamp = 
        ((uint32_t)buffer[0]) |
        ((uint32_t)buffer[1] << 8) |
        ((uint32_t)buffer[2] << 16) |
        ((uint32_t)buffer[3] << 24);
    
    record->station_id = 
        ((uint16_t)buffer[4]) |
        ((uint16_t)buffer[5] << 8);
    
    record->temperature = (int16_t)(
        ((uint16_t)buffer[6]) |
        ((uint16_t)buffer[7] << 8));
    
    record->et = (int16_t)(
        ((uint16_t)buffer[8]) |
        ((uint16_t)buffer[9] << 8));
    
    record->wind_speed = 
        ((uint16_t)buffer[10]) |
        ((uint16_t)buffer[11] << 8);
    
    record->wind_direction = buffer[12];
    record->humidity = buffer[13];
    
    record->solar_radiation = 
        ((uint16_t)buffer[14]) |
        ((uint16_t)buffer[15] << 8);
    
    record->precipitation = 
        ((uint16_t)buffer[16]) |
        ((uint16_t)buffer[17] << 8);
    
    record->vapor_pressure = 
        ((uint16_t)buffer[18]) |
        ((uint16_t)buffer[19] << 8);
    
    record->qc_flags = buffer[20];
    record->reserved = buffer[21];
    record->pad[0] = buffer[22];
    record->pad[1] = buffer[23];
    
    return CIMIS_OK;
}

/* Encode a batch of hourly records */
size_t cimis_encode_hourly_batch(const cimis_hourly_record_t *records, uint32_t count,
                                 uint8_t *buffer, size_t buffer_size) {
    if (records == NULL || buffer == NULL || count == 0) {
        return 0;
    }
    
    size_t required_size = count * CIMIS_HOURLY_RECORD_SIZE;
    if (buffer_size < required_size) {
        return 0;
    }
    
    for (uint32_t i = 0; i < count; i++) {
        cimis_result_t result = cimis_encode_hourly_record(
            &records[i],
            buffer + (i * CIMIS_HOURLY_RECORD_SIZE),
            CIMIS_HOURLY_RECORD_SIZE
        );
        if (result != CIMIS_OK) {
            return 0;
        }
    }
    
    return required_size;
}

/* Decode a batch of hourly records */
size_t cimis_decode_hourly_batch(const uint8_t *buffer, size_t buffer_size,
                                   cimis_hourly_record_t *records, uint32_t max_count) {
    if (buffer == NULL || records == NULL || max_count == 0) {
        return 0;
    }
    
    uint32_t record_count = buffer_size / CIMIS_HOURLY_RECORD_SIZE;
    if (record_count > max_count) {
        record_count = max_count;
    }
    
    for (uint32_t i = 0; i < record_count; i++) {
        cimis_result_t result = cimis_decode_hourly_record(
            buffer + (i * CIMIS_HOURLY_RECORD_SIZE),
            CIMIS_HOURLY_RECORD_SIZE,
            &records[i]
        );
        if (result != CIMIS_OK) {
            return i;
        }
    }
    
    return record_count;
}

/* Validate a daily record */
bool cimis_validate_daily_record(const cimis_daily_record_t *record) {
    if (record == NULL) {
        return false;
    }
    
    /* Check station ID is valid (1-65535) */
    if (record->station_id == 0) {
        return false;
    }
    
    /* Check timestamp is reasonable (1985-2035) */
    if (record->timestamp > 18250) { /* ~50 years */
        return false;
    }
    
    /* Check temperature is in valid range (-50 to 60°C, scaled) */
    if (record->temperature < -500 || record->temperature > 600) {
        return false;
    }
    
    /* Check humidity is valid (0-100%) */
    if (record->humidity > 100) {
        return false;
    }
    
    return true;
}

/* Validate an hourly record */
bool cimis_validate_hourly_record(const cimis_hourly_record_t *record) {
    if (record == NULL) {
        return false;
    }
    
    /* Check station ID is valid */
    if (record->station_id == 0) {
        return false;
    }
    
    /* Check timestamp is reasonable */
    if (record->timestamp > 438000) { /* 50 years * 365 * 24 hours */
        return false;
    }
    
    /* Check temperature range */
    if (record->temperature < -500 || record->temperature > 600) {
        return false;
    }
    
    /* Check humidity is valid */
    if (record->humidity > 100) {
        return false;
    }
    
    return true;
}

/* Initialize iterator */
cimis_result_t cimis_iterator_init(cimis_record_iterator_t *iter, const uint8_t *buffer, 
                                     size_t buffer_size, bool is_hourly) {
    if (iter == NULL || buffer == NULL) {
        return CIMIS_ERR_NULL_PTR;
    }
    
    iter->buffer = buffer;
    iter->buffer_size = buffer_size;
    iter->current_offset = 0;
    iter->is_hourly = is_hourly;
    
    size_t record_size = is_hourly ? CIMIS_HOURLY_RECORD_SIZE : CIMIS_DAILY_RECORD_SIZE;
    iter->record_count = buffer_size / record_size;
    
    return CIMIS_OK;
}

/* Check if iterator has more records */
bool cimis_iterator_has_next(const cimis_record_iterator_t *iter) {
    if (iter == NULL) {
        return false;
    }
    
    size_t record_size = iter->is_hourly ? CIMIS_HOURLY_RECORD_SIZE : CIMIS_DAILY_RECORD_SIZE;
    return iter->current_offset + record_size <= iter->buffer_size;
}

/* Get next daily record from iterator */
cimis_result_t cimis_iterator_next_daily(cimis_record_iterator_t *iter, cimis_daily_record_t *record) {
    if (iter == NULL || record == NULL) {
        return CIMIS_ERR_NULL_PTR;
    }
    
    if (iter->is_hourly) {
        return CIMIS_ERR_INVALID_SIZE;
    }
    
    if (!cimis_iterator_has_next(iter)) {
        return CIMIS_ERR_INVALID_SIZE;
    }
    
    cimis_result_t result = cimis_decode_daily_record(
        iter->buffer + iter->current_offset,
        CIMIS_DAILY_RECORD_SIZE,
        record
    );
    
    if (result == CIMIS_OK) {
        iter->current_offset += CIMIS_DAILY_RECORD_SIZE;
    }
    
    return result;
}

/* Get next hourly record from iterator */
cimis_result_t cimis_iterator_next_hourly(cimis_record_iterator_t *iter, cimis_hourly_record_t *record) {
    if (iter == NULL || record == NULL) {
        return CIMIS_ERR_NULL_PTR;
    }
    
    if (!iter->is_hourly) {
        return CIMIS_ERR_INVALID_SIZE;
    }
    
    if (!cimis_iterator_has_next(iter)) {
        return CIMIS_ERR_INVALID_SIZE;
    }
    
    cimis_result_t result = cimis_decode_hourly_record(
        iter->buffer + iter->current_offset,
        CIMIS_HOURLY_RECORD_SIZE,
        record
    );
    
    if (result == CIMIS_OK) {
        iter->current_offset += CIMIS_HOURLY_RECORD_SIZE;
    }
    
    return result;
}

/* Calculate statistics for daily records */
void cimis_calculate_daily_stats(const cimis_daily_record_t *records, uint32_t count, cimis_daily_stats_t *stats) {
    if (records == NULL || stats == NULL || count == 0) {
        return;
    }
    
    float min_temp = 1000.0f;
    float max_temp = -1000.0f;
    float sum_temp = 0.0f;
    float total_et = 0.0f;
    
    for (uint32_t i = 0; i < count; i++) {
        float temp = cimis_fixed_to_float_temp(records[i].temperature);
        float et = cimis_fixed_to_float_et_daily(records[i].et);
        
        if (temp < min_temp) min_temp = temp;
        if (temp > max_temp) max_temp = temp;
        sum_temp += temp;
        total_et += et;
    }
    
    stats->min_temp = min_temp;
    stats->max_temp = max_temp;
    stats->avg_temp = sum_temp / count;
    stats->total_et = total_et;
    stats->record_count = count;
}
