SELECT Manufacturer.ManufacturerID, SUM(Service.Cost)
FROM Car, Manufacturer, Service
WHERE Car.ManufacturerID = Manufacturer.ManufacturerID
  AND Car.CarID = Service.CarID
  AND Manufacturer.ManufacturerID = 1
GROUP BY Manufacturer.ManufacturerID;