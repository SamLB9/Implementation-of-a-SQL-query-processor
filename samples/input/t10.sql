SELECT Airline.Name, SUM(Flight.Price)
FROM Flight, Airline
WHERE Flight.AirlineID = Airline.AirlineID
GROUP BY Airline.Name;