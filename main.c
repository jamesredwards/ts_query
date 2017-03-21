//
//  main.c
//  ts_query
//
//  Created by James Edwards on 11/03/2017.
//  Copyright © 2017 James Edwards. All rights reserved.
//

#include <stdio.h>
#include <sqlite3.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

#define MAX_SIZE (365 * 30)

static int SUMMARISE_PERF = 0;
static double *values;
static int call_back_ctr = 0;
static struct tm* start_date;
static struct tm* end_date;

int callback(void *, int, char **, char **);

const char* parse_date(const char*, struct tm*);

double parse_double(char*);

void cmd_line_usage();

double stddev(double*, int);


int main(int argc, const char * argv[]) {
    
    /*argc = 3;
    char *ticker = "ISF.L";
    SUMMARISE_PERF = 1;*/
    
    if (argc < 2) {
        cmd_line_usage();
        return 0;
    }
    
    const char *ticker = argv[1];

    
    if (argc == 3 && strcmp(argv[2], "-s") == 0) {
        SUMMARISE_PERF = 1;
    }
    
    if (SUMMARISE_PERF) {
        values = malloc(sizeof(double) * MAX_SIZE);
    }
    
    sqlite3 *db;
    char *err_msg = 0;
    
    char *conn_string = "//Users//jamesedwards//documents//investments//idx_r.db";
    
    int rc = sqlite3_open(conn_string, &db);
    
    if (rc != SQLITE_OK) {
        
        fprintf(stderr, "Cannot open database: %s\n",
                sqlite3_errmsg(db));
        sqlite3_close(db);
        
        return 1;
    }
    
    char *sql = malloc(sizeof(char) * 100);
    strcpy(sql, "select date, value from time_series where ticker = \'");
    strcat(sql, ticker);
    strcat(sql, "\'");
    
    start_date = malloc(sizeof(*start_date));
    end_date = malloc(sizeof(*end_date));
    
    rc = sqlite3_exec(db, sql, callback, 0, &err_msg);
    
    if (rc != SQLITE_OK ) {
        
        fprintf(stderr, "Failed to select data\n");
        fprintf(stderr, "SQL error: %s\n", err_msg);
        
        sqlite3_free(err_msg);
        sqlite3_close(db);
        
        return 1;
    }
    
    sqlite3_close(db);
    
    if (SUMMARISE_PERF) {
        
        char *start_date_str = malloc(sizeof(char) * 12);
        char *end_date_str = malloc(sizeof(char) * 12);
        
        strftime(start_date_str, 12, "%d %h %Y", start_date);
        strftime(end_date_str, 12, "%d %h %Y", end_date);
        
        int *tmp = realloc(values, call_back_ctr * sizeof(double));
        
        if(tmp) {
            
            double *rets = malloc(sizeof(double) * (call_back_ctr-1));
            double product = 1;
            
            for (int i = 1; i < call_back_ctr-1; i++) {
                rets[i-1] = (values[i] / values[i-1]) - 1.0;
                product *= (1.0 + rets[i-1]);
                
            }
            
            double std = stddev(rets, call_back_ctr-1);
            double cum_ret = product - 1;
            double annualise_pow = (double)(call_back_ctr / 365);
            double ann_ret = pow(product, 1/annualise_pow)-1;
            std = std * (double)sqrt(365);
            double sharpe = ann_ret / std;
            
            printf("Summary performance for %s for the period %s to %s:\n\n", ticker, start_date_str, end_date_str);
            printf("Cumulative return: %0.2f%%\n", cum_ret * 100);
            printf("Annualised return: %0.2f%%\n", ann_ret * 100);
            printf("Volatility       : %0.2f%%\n", std * 100);
            printf("Sharpe Ratio     : %0.2f\n", sharpe);
            free(rets);
        }
        
        free(tmp);
        free(start_date_str);
        free(end_date_str);
        free(end_date);
        free(start_date);
    }
    
    return 0;
}

int callback(void *NotUsed, int num_cols, char **results, char **column_names)
{
    NotUsed = 0;
    
    int date_col = 0;
    int val_col = 1;
    
    double value = 0.0;
    
    if (SUMMARISE_PERF == 0) {
        if (call_back_ctr == 0) {
            //print column names
            for (int i = 0; i < num_cols; i++)
            {
                if (i == (num_cols - 1)) {
                    printf("%s\n", column_names[i]);
                }
                else {
                    printf("%-15s", column_names[i]);
                }
            }
        }
        
        for (int i = 0; i < num_cols; i++)
        {
            if (i == date_col) {
                if (results[i]) {
                    char *date_str = malloc(sizeof(char) * 10);
                    strncpy(date_str, results[i], 10);
                    printf("%-15s", date_str);
                }
            }
            if (i == val_col) {
                value = parse_double(results[i]);
                printf("%.6f\n", value);
                
            }
        }
    }
    else {
        if (call_back_ctr == 0) {
            parse_date(results[date_col], start_date);
        }
        if (call_back_ctr > 0) {
            parse_date(results[date_col], end_date);
            value = parse_double(results[val_col]);
            values[call_back_ctr-1] = value;
        }
    }
    call_back_ctr++;
    return 0;
}

const char* parse_date(const char* input, struct tm* date)
{
    const char *cp;
    
    char *format = "%Y-%m-%d %H:%M:%S";
    
    cp = strptime (input, format, date);
    
    if (cp == NULL) {
        fprintf(stderr, "Error parsing date %s", input);
    }
    
    return cp;
}

double parse_double(char* input)
{
    double d;
    d = strtod(input, NULL);
    return d;
}

double stddev(double *values, int len)
{
    double sum = 0.0;
    for (int i = 0; i < len; i++) {
        sum += values[i];
    }
    double mean = (double)(sum / len);
    
    double accum = 0.0;
    
    for (int i = 0; i < len; i++) {
        accum += pow(values[i] - mean, 2);
    }
    
    double variance = (double)(accum / len);
    return sqrt(variance);
}

void cmd_line_usage()
{
    printf("Usage: ts_query 'TICKER' [-s]\n");
    printf("By default will print all time series data for the given ticker.\n");
    printf("If -s is used, a summary of performance since inception will be shown.\n");
}
