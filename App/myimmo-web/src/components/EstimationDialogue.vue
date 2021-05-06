<template>
  <div class="text-center">
    <v-dialog
      v-model="dialog"
      width="500"
    >
      <template v-slot:activator="{ on, attrs }">
        <v-btn
            color="primary"
            dark
            large
            v-bind="attrs"
            v-on="on"
            v-on:click="estimate()"
            
          >
            Estimer mon bien - €
          </v-btn>
      </template>

      <v-card>
        <v-img
            height="250"
            src="https://cdn.vuetifyjs.com/images/cards/house.jpg"
        ></v-img>
        <v-card-title>
            <div class="display-1 mb-2">
                Mon bien immobilier
            </div>
            <div class="title font-weight-regular grey--text" v-if="infoAdresse != undefined">
             {{ infoAdresse.properties.label }}
            </div>
            <div class="title font-weight-regular grey--text" v-if="infoAdresse != undefined">
             long: {{ infoAdresse.geometry.coordinates[0] }}, lat: {{infoAdresse.geometry.coordinates[1]}}
            </div>
        </v-card-title>

        <v-divider class="mt-6 mx-4"></v-divider>

        <v-card-title>Estimation</v-card-title>
        <v-card-text>
            <v-row
            justify="center"
            align="center">
                <v-chip 
                class="text-center ma-2"
                color="green"
                outlined
                large
                >
                    <v-icon left>mdi-currency-eur</v-icon>
                    {{ valueEstimate }} /m²
                </v-chip>
            </v-row>
        </v-card-text>

        <v-divider></v-divider>

        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn
            color="primary"
            text
            @click="dialog = false"
          >
            Fermer
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<script>
export default {
    data () {
      return {
        dialog: false,
        valueEstimate: 1500
      }
    },
    props: {
        infoAdresse: Object,
        surface: String
    },
    methods: {
        estimate () {
            console.log(this.infoAdresse)
            console.log(this.surface)
        }
    }
}
</script>

<style>

</style>