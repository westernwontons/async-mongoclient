db.example.insertMany([
	{
		user: 'Benji',
		password: 'yeet',
		nested: [
			{
				attrs: ['one', 'two', 'three'],
			},
			{
				attrs: ['four', 'five', 'six'],
			},
		],
	},
	{
		user: 'Timi',
		password: 'sweatherweather',
		nested: [
			{
				attrs: ['seven', 'eight', 'nine'],
			},
			{
				attrs: ['ten', 'eleven', 'twelve'],
			},
		],
	},
]);
