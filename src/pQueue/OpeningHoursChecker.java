package pQueue;

import de.starface.core.component.StarfaceComponentProvider;
import de.vertico.starface.module.core.model.VariableType;
import de.vertico.starface.module.core.model.Visibility;
import de.vertico.starface.module.core.runtime.IAGIJavaExecutable;
import de.vertico.starface.module.core.runtime.IAGIRuntimeEnvironment;
import de.vertico.starface.module.core.runtime.annotations.Function;
import de.vertico.starface.module.core.runtime.annotations.InputVar;
import de.vertico.starface.module.core.runtime.annotations.OutputVar;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;




@Function(visibility= Visibility.Private, rookieFunction=true, description="Default")
public class OpeningHoursChecker implements IAGIJavaExecutable {

    @InputVar(label="Oeffnungzeiten", description="Oeffnungszeiten",type= VariableType.LIST)
    public ArrayList<ArrayList<String>> openingHours;

    @InputVar(label="Geschlossen", description="Geschlossen Datum",type= VariableType.LIST)
    public ArrayList<ArrayList<String>> closedDays;

    @OutputVar(label="Ist geoeffnet", description="Ergebnis der Pruefung",type= VariableType.BOOLEAN)
    public boolean isOpen = false;

    StarfaceComponentProvider componentProvider = StarfaceComponentProvider.getInstance();

    @Override
    public void execute(IAGIRuntimeEnvironment context) throws Exception {
        Logger log = context.getLog();

        log.trace("openingHours: {} {}",  openingHours.getClass().getName(), openingHours);
        log.trace("closedDays: {} {}",  closedDays.getClass().getName(), closedDays);

        // is closed today
        LocalDateTime now = LocalDateTime.now();
        for(String closedDay : closedDays.get(0)){
            // Parse the date with the format dd.mm.yyyy
            LocalDate date = LocalDate.parse(closedDay, DateTimeFormatter.ofPattern("dd.MM.yyyy"));

            // Check if the date is today
            if(date.getDayOfMonth() == now.getDayOfMonth() && date.getMonthValue() == now.getMonthValue() && date.getYear() == now.getYear()){
                log.debug("Ergebnis: derzeit geschlossen weil closedDays.");
                return;
            }
        }
        log.debug("Ergebnis: derzeit nicht geschlossen weil closedDays.");


        // parse opening hours
        Map<DayOfWeek, ArrayList<TimeRange>> parsedHours = parseOpeningHours(openingHours.get(0), log);


        // check if open
        isOpen = isOpenAt(parsedHours, now);

        // log result
        if (isOpen) {
            log.debug("Ergebnis: derzeit geöffnet.");
        } else {
            log.debug("Ergebnis: derzeit geschlossen.");
        }


    }


    public static Map<DayOfWeek, ArrayList<TimeRange>> parseOpeningHours(ArrayList<String> openingHoursL, Logger log){
        Map<DayOfWeek, ArrayList<TimeRange>> openingHoursMap = new HashMap<>();

        log.trace("Parsing opening hours: {} {}",  openingHoursL.getClass().getName(), openingHoursL);


        for (String hours : openingHoursL) {
            log.trace("Parsing opening hours: {}", hours);

            String[] parts = hours.split(" ");
            String days = parts[0];
            String timeRange = parts[1];

            String[] timeParts = timeRange.split("-");
            LocalTime startTime = LocalTime.parse(timeParts[0]);
            LocalTime endTime = LocalTime.parse(timeParts[1]);

            TimeRange range = new TimeRange(startTime, endTime);

            if (days.contains("-")) {
                log.trace("Parsing day range: {}", days);
                String[] dayParts = days.split("-");
                DayOfWeek startDay = parseDay(dayParts[0]);
                DayOfWeek endDay = parseDay(dayParts[1]);

                for (DayOfWeek day = startDay; day.compareTo(endDay) <= 0; day = day.plus(1)) {
                    if (openingHoursMap.containsKey(day)) {
                        openingHoursMap.get(day).add(range);
                    } else {
                        openingHoursMap.put(day, new ArrayList<>(Arrays.asList(range)));
                    }
                }
            } else {
                log.trace("Parsing single day: {}", days);
                DayOfWeek day = parseDay(days);
                if (openingHoursMap.containsKey(day)) {
                    openingHoursMap.get(day).add(range);
                } else {
                    openingHoursMap.put(day, new ArrayList<>(Arrays.asList(range)));
                }
            }
        }

        return openingHoursMap;
    }

    public static DayOfWeek parseDay(String day) {
        switch (day) {
            case "Mo": return DayOfWeek.MONDAY;
            case "Di": return DayOfWeek.TUESDAY;
            case "Mi": return DayOfWeek.WEDNESDAY;
            case "Do": return DayOfWeek.THURSDAY;
            case "Fr": return DayOfWeek.FRIDAY;
            case "Sa": return DayOfWeek.SATURDAY;
            case "So": return DayOfWeek.SUNDAY;
            default: throw new IllegalArgumentException("Unbekannter Tag: " + day);
        }
    }

    public static boolean isOpenAt(Map<DayOfWeek, ArrayList<TimeRange>> openingHoursL, LocalDateTime dateTime) {
        DayOfWeek day = dateTime.getDayOfWeek();
        LocalTime time = dateTime.toLocalTime();

        ArrayList<TimeRange> ranges = openingHoursL.get(day);
        if (ranges != null) {
            for (TimeRange range : ranges) {
                if (range.isWithinRange(time)) {
                    return true;
                }
            }
        }

        return false;
    }

}


