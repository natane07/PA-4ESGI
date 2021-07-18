console.log('Loading function');

exports.handler = async (event, context) => {
    console.log('Received event:', JSON.stringify(event, null, 2));
    console.log('value1 =', event.key1);
    console.log('value2 =', event.key2);
    console.log('value3 =', event.key3);
    var key1 = 0
    if (event.body) {
        let body = JSON.parse(event.body)
        if (body.key1) 
            key1 = body.key1;
    }
    return key1;  // Echo back the first key value
    // throw new Error('Something went wrong');
};