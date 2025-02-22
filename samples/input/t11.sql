SELECT Flight.FlightID, SUM(Flight.Price)
FROM Flight
GROUP BY Flight.FlightID
ORDER BY SUM(Flight.Price);