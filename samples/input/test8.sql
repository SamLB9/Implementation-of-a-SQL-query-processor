SELECT Car.Model, SUM(Car.Price)
FROM Car
GROUP BY Car.Model
ORDER BY SUM(Car.Price);