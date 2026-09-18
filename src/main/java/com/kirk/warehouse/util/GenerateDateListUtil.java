package com.kirk.warehouse.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public final class GenerateDateListUtil {
    private GenerateDateListUtil(){}

    public static final DateTimeFormatter DT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static final List<String> generateDateList(String start, String end){
        List<String> dates = new ArrayList<String>();
        LocalDate startDate = LocalDate.parse(start, DT_FORMAT);
        LocalDate endDate = LocalDate.parse(end, DT_FORMAT);
        LocalDate current = startDate;

        while (!current.isAfter(endDate)){
            dates.add(current.toString());
            current = current.plusDays(1);
        }
        return dates;
    }
}
