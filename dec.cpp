/* Convert hex to decimal */

#include <iostream>
#include <cmath>
#include <string>

using namespace std;

long long toDecimal(string hex) {
    long long decimal, place;
    int i = 0, val, len;

    decimal = 0;
    place = 1;

    len = hex.length();
    len--;

    for(i=0; i < hex.length(); i++)
    {
        if(hex[i]>='0' && hex[i]<='9')
        {
            val = hex[i] - 48;
        }
        else if(hex[i]>='a' && hex[i]<='f')
        {
            val = hex[i] - 97 + 10;
        }
        else if(hex[i]>='A' && hex[i]<='F')
        {
            val = hex[i] - 65 + 10;
        }

        decimal += val * pow(16, len);
        len--;
    }

    return decimal;
}


int main (int argc, char **argv)
{
    string hex;

    if (argc > 2) cerr << "usage: dec [hex_value]" << endl;
    else if (argc == 2) cout << toDecimal(argv[1]) << endl;
    else while (cin >> hex) cout << toDecimal(hex) << endl;

    return 0;
}
