SELECT *
FROM Car, Manufacturer, Service
WHERE Car.ManufacturerID = Manufacturer.ManufacturerID
  AND Car.CarID = Service.CarID
ORDER BY Car.Price;